#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Native Tyrion values are opaque 128-byte slots. Keep this helper at the
 * public runtime boundary: it uses the same lifecycle and collection entry
 * points as generated Tyrion code and therefore does not duplicate ownership
 * rules. */
typedef struct TyCacheSlot {
    unsigned char bytes[128];
} TyCacheSlot;

#if defined(__x86_64__) && defined(__linux__)
extern void ty_cache_runtime_slot_release(TyCacheSlot *) __asm__("__tyrion_direct_slot_release");
#else
extern void ty_cache_runtime_slot_release(TyCacheSlot *) __asm__("__tyrion_app_slot_release");
#endif

enum {
    TY_CACHE_TAG_NONE = 0,
    TY_CACHE_TAG_INT = 2,
    TY_CACHE_TAG_BOOL = 3,
    TY_CACHE_TAG_STRING_INLINE = 1,
    TY_CACHE_TAG_STRING_BORROWED = 4,
    TY_CACHE_TAG_STRING_HEAP = 8,
    TY_CACHE_TAG_LIST = 9,
    TY_CACHE_TAG_DICT = 10,
    TY_CACHE_TAG_FLOAT = 19,
};

typedef struct TyCacheBuffer {
    char *data;
    size_t length;
    size_t capacity;
} TyCacheBuffer;

typedef struct TyCacheReader {
    const char *data;
    size_t length;
    size_t position;
} TyCacheReader;

static uint64_t ty_cache_load_u64(const TyCacheSlot *slot, size_t offset) {
    uint64_t value = 0;
    memcpy(&value, slot->bytes + offset, sizeof(value));
    return value;
}

static void ty_cache_store_u64(TyCacheSlot *slot, size_t offset, uint64_t value) {
    memcpy(slot->bytes + offset, &value, sizeof(value));
}

static unsigned char ty_cache_tag(const TyCacheSlot *slot) {
    return slot->bytes[127];
}

static void ty_cache_slot_init(TyCacheSlot *slot) {
    memset(slot, 0, sizeof(*slot));
}

static void ty_cache_slot_release(TyCacheSlot *slot) {
    ty_cache_runtime_slot_release(slot);
}

static void ty_cache_list_new(TyCacheSlot *out, uint64_t capacity) {
    ty_cache_slot_release(out);
    TyCacheSlot *items = capacity ? calloc((size_t)capacity, sizeof(*items)) : NULL;
    if (capacity && !items) return;
    ty_cache_store_u64(out, 0, (uint64_t)(uintptr_t)items);
    ty_cache_store_u64(out, 8, 0);
    ty_cache_store_u64(out, 16, capacity);
    out->bytes[126] = 2;
    out->bytes[127] = TY_CACHE_TAG_LIST;
}

static void ty_cache_list_set(TyCacheSlot *out, uint64_t index, TyCacheSlot *value) {
    uint64_t capacity = ty_cache_load_u64(out, 16);
    TyCacheSlot *items = (TyCacheSlot *)(uintptr_t)ty_cache_load_u64(out, 0);
    if (!items || index >= capacity) return;
    memcpy(&items[index], value, sizeof(*value));
    ty_cache_slot_init(value);
    if (index >= ty_cache_load_u64(out, 8)) ty_cache_store_u64(out, 8, index + 1);
}

static void ty_cache_dict_new(TyCacheSlot *out, uint64_t capacity) {
    ty_cache_slot_release(out);
    TyCacheSlot *items = capacity ? calloc((size_t)capacity * 2, sizeof(*items)) : NULL;
    if (capacity && !items) return;
    ty_cache_store_u64(out, 0, (uint64_t)(uintptr_t)items);
    ty_cache_store_u64(out, 8, 0);
    ty_cache_store_u64(out, 16, capacity);
    out->bytes[126] = 2;
    out->bytes[127] = TY_CACHE_TAG_DICT;
}

static void ty_cache_dict_set(TyCacheSlot *out, TyCacheSlot *key, TyCacheSlot *value) {
    uint64_t count = ty_cache_load_u64(out, 8);
    uint64_t capacity = ty_cache_load_u64(out, 16);
    TyCacheSlot *items = (TyCacheSlot *)(uintptr_t)ty_cache_load_u64(out, 0);
    if (!items || count >= capacity) return;
    memcpy(&items[count * 2], key, sizeof(*key));
    memcpy(&items[count * 2 + 1], value, sizeof(*value));
    ty_cache_slot_init(key);
    ty_cache_slot_init(value);
    ty_cache_store_u64(out, 8, count + 1);
}

static const char *ty_cache_string_data(const TyCacheSlot *slot) {
    if (ty_cache_tag(slot) == TY_CACHE_TAG_STRING_INLINE) return (const char *)slot->bytes;
    if (ty_cache_tag(slot) == TY_CACHE_TAG_STRING_BORROWED || ty_cache_tag(slot) == TY_CACHE_TAG_STRING_HEAP) {
        return (const char *)(uintptr_t)ty_cache_load_u64(slot, 0);
    }
    return NULL;
}

static size_t ty_cache_string_length(const TyCacheSlot *slot) {
    if (ty_cache_tag(slot) == TY_CACHE_TAG_STRING_INLINE) {
        size_t length = 0;
        while (length <= 120 && slot->bytes[length] != '\0') length++;
        return length;
    }
    return (size_t)ty_cache_load_u64(slot, 8);
}

static int ty_cache_buffer_append(TyCacheBuffer *buffer, const void *data, size_t length) {
    if (!buffer || (!data && length != 0) || length > SIZE_MAX - buffer->length - 1) return 0;
    size_t required = buffer->length + length + 1;
    if (required > buffer->capacity) {
        size_t capacity = buffer->capacity ? buffer->capacity : 256;
        while (capacity < required) {
            if (capacity > SIZE_MAX / 2) return 0;
            capacity *= 2;
        }
        char *grown = (char *)realloc(buffer->data, capacity);
        if (!grown) return 0;
        buffer->data = grown;
        buffer->capacity = capacity;
    }
    if (length != 0) memcpy(buffer->data + buffer->length, data, length);
    buffer->length += length;
    buffer->data[buffer->length] = '\0';
    return 1;
}

static int ty_cache_buffer_cstr(TyCacheBuffer *buffer, const char *text) {
    return ty_cache_buffer_append(buffer, text, text ? strlen(text) : 0);
}

static int ty_cache_buffer_size(TyCacheBuffer *buffer, size_t value) {
    char text[32];
    int length = snprintf(text, sizeof(text), "%zu", value);
    return length > 0 && (size_t)length < sizeof(text) &&
           ty_cache_buffer_append(buffer, text, (size_t)length);
}

static int ty_cache_encode_value(const TyCacheSlot *value, TyCacheBuffer *buffer, unsigned depth) {
    if (!value || !buffer || depth > 256) return 0;
    unsigned char tag = ty_cache_tag(value);
    if (tag == TY_CACHE_TAG_NONE) return ty_cache_buffer_cstr(buffer, "N;");
    if (tag == TY_CACHE_TAG_BOOL) {
        return ty_cache_buffer_cstr(buffer, ty_cache_load_u64(value, 0) ? "B1;" : "B0;");
    }
    if (tag == TY_CACHE_TAG_INT) {
        char text[64];
        int length = snprintf(text, sizeof(text), "I%lld;", (long long)ty_cache_load_u64(value, 0));
        return length > 0 && (size_t)length < sizeof(text) &&
               ty_cache_buffer_append(buffer, text, (size_t)length);
    }
    if (tag == TY_CACHE_TAG_FLOAT) {
        double number = 0.0;
        char text[96];
        memcpy(&number, value->bytes, sizeof(number));
        int length = snprintf(text, sizeof(text), "F%a;", number);
        return length > 0 && (size_t)length < sizeof(text) &&
               ty_cache_buffer_append(buffer, text, (size_t)length);
    }
    if (tag == TY_CACHE_TAG_STRING_INLINE || tag == TY_CACHE_TAG_STRING_BORROWED || tag == TY_CACHE_TAG_STRING_HEAP) {
        const char *data = ty_cache_string_data(value);
        size_t length = ty_cache_string_length(value);
        return data && ty_cache_buffer_cstr(buffer, "S") &&
               ty_cache_buffer_size(buffer, length) &&
               ty_cache_buffer_cstr(buffer, ":") &&
               ty_cache_buffer_append(buffer, data, length);
    }
    if (tag == TY_CACHE_TAG_LIST) {
        const TyCacheSlot *items = (const TyCacheSlot *)(uintptr_t)ty_cache_load_u64(value, 0);
        size_t count = (size_t)ty_cache_load_u64(value, 8);
        if ((count != 0 && !items) || !ty_cache_buffer_cstr(buffer, "L") ||
            !ty_cache_buffer_size(buffer, count) || !ty_cache_buffer_cstr(buffer, ";")) return 0;
        for (size_t index = 0; index < count; index++) {
            if (!ty_cache_encode_value(&items[index], buffer, depth + 1)) return 0;
        }
        return 1;
    }
    if (tag == TY_CACHE_TAG_DICT) {
        const unsigned char *entries = (const unsigned char *)(uintptr_t)ty_cache_load_u64(value, 0);
        size_t count = (size_t)ty_cache_load_u64(value, 8);
        if ((count != 0 && !entries) || !ty_cache_buffer_cstr(buffer, "D") ||
            !ty_cache_buffer_size(buffer, count) || !ty_cache_buffer_cstr(buffer, ";")) return 0;
        for (size_t index = 0; index < count; index++) {
            const TyCacheSlot *key = (const TyCacheSlot *)(entries + index * 256);
            const TyCacheSlot *item = (const TyCacheSlot *)(entries + index * 256 + 128);
            if (!ty_cache_encode_value(key, buffer, depth + 1) ||
                !ty_cache_encode_value(item, buffer, depth + 1)) return 0;
        }
        return 1;
    }
    return 0;
}

static int ty_cache_reader_byte(TyCacheReader *reader, char expected) {
    if (!reader || reader->position >= reader->length || reader->data[reader->position] != expected) return 0;
    reader->position++;
    return 1;
}

static int ty_cache_reader_size(TyCacheReader *reader, size_t *out) {
    if (!reader || !out || reader->position >= reader->length) return 0;
    size_t value = 0;
    size_t start = reader->position;
    while (reader->position < reader->length && reader->data[reader->position] >= '0' && reader->data[reader->position] <= '9') {
        size_t digit = (size_t)(reader->data[reader->position] - '0');
        if (value > (SIZE_MAX - digit) / 10) return 0;
        value = value * 10 + digit;
        reader->position++;
    }
    if (reader->position == start) return 0;
    *out = value;
    return 1;
}

static void ty_cache_set_none(TyCacheSlot *out) {
    ty_cache_slot_release(out);
    ty_cache_slot_init(out);
}

static void ty_cache_set_scalar(TyCacheSlot *out, unsigned char tag, uint64_t bits) {
    ty_cache_set_none(out);
    ty_cache_store_u64(out, 0, bits);
    out->bytes[127] = tag;
}

static int ty_cache_set_string(TyCacheSlot *out, const char *data, size_t length) {
    if (!out || (!data && length != 0)) return 0;
    ty_cache_set_none(out);
    if (length == SIZE_MAX) return 0;
    char *copy = (char *)malloc(length + 1);
    if (!copy) return 0;
    memcpy(copy, data, length);
    copy[length] = '\0';
    ty_cache_store_u64(out, 0, (uint64_t)(uintptr_t)copy);
    ty_cache_store_u64(out, 8, length);
    out->bytes[127] = TY_CACHE_TAG_STRING_HEAP;
    return 1;
}

static int ty_cache_decode_value(TyCacheReader *reader, TyCacheSlot *out, unsigned depth) {
    if (!reader || !out || depth > 256 || reader->position >= reader->length) return 0;
    char tag = reader->data[reader->position++];
    if (tag == 'N') {
        if (!ty_cache_reader_byte(reader, ';')) return 0;
        ty_cache_set_none(out);
        return 1;
    }
    if (tag == 'B') {
        if (reader->position >= reader->length ||
            (reader->data[reader->position] != '0' && reader->data[reader->position] != '1')) return 0;
        uint64_t value = reader->data[reader->position++] == '1';
        if (!ty_cache_reader_byte(reader, ';')) return 0;
        ty_cache_set_scalar(out, TY_CACHE_TAG_BOOL, value);
        return 1;
    }
    if (tag == 'I' || tag == 'F') {
        size_t start = reader->position;
        while (reader->position < reader->length && reader->data[reader->position] != ';') reader->position++;
        if (reader->position == start || reader->position >= reader->length) return 0;
        size_t length = reader->position - start;
        char text[128];
        if (length >= sizeof(text)) return 0;
        memcpy(text, reader->data + start, length);
        text[length] = '\0';
        reader->position++;
        if (tag == 'I') {
            char *end = NULL;
            long long value = strtoll(text, &end, 10);
            if (!end || *end != '\0') return 0;
            ty_cache_set_scalar(out, TY_CACHE_TAG_INT, (uint64_t)value);
        } else {
            char *end = NULL;
            double value = strtod(text, &end);
            uint64_t bits = 0;
            if (!end || *end != '\0') return 0;
            memcpy(&bits, &value, sizeof(bits));
            ty_cache_set_scalar(out, TY_CACHE_TAG_FLOAT, bits);
        }
        return 1;
    }
    if (tag == 'S') {
        size_t length = 0;
        if (!ty_cache_reader_size(reader, &length) || !ty_cache_reader_byte(reader, ':') ||
            length > reader->length - reader->position) return 0;
        int ok = ty_cache_set_string(out, reader->data + reader->position, length);
        reader->position += length;
        return ok;
    }
    if (tag == 'L' || tag == 'D' || tag == 'A') {
        size_t count = 0;
        if (!ty_cache_reader_size(reader, &count) || !ty_cache_reader_byte(reader, ';') || count > 1000000) return 0;
        ty_cache_set_none(out);
        if (tag == 'L') ty_cache_list_new(out, (uint64_t)count);
        else ty_cache_dict_new(out, (uint64_t)count);
        if ((tag == 'L' && ty_cache_tag(out) != TY_CACHE_TAG_LIST) ||
            (tag != 'L' && ty_cache_tag(out) != TY_CACHE_TAG_DICT)) return 0;
        for (size_t index = 0; index < count; index++) {
            TyCacheSlot key;
            TyCacheSlot value;
            ty_cache_slot_init(&key);
            ty_cache_slot_init(&value);
            if (tag == 'L') {
                if (!ty_cache_decode_value(reader, &value, depth + 1)) {
                    ty_cache_slot_release(&value);
                    ty_cache_slot_release(out);
                    return 0;
                }
                ty_cache_list_set(out, (uint64_t)index, &value);
            } else {
                if (!ty_cache_decode_value(reader, &key, depth + 1) ||
                    (ty_cache_tag(&key) != TY_CACHE_TAG_STRING_INLINE && ty_cache_tag(&key) != TY_CACHE_TAG_STRING_HEAP) ||
                    !ty_cache_decode_value(reader, &value, depth + 1)) {
                    ty_cache_slot_release(&key);
                    ty_cache_slot_release(&value);
                    ty_cache_slot_release(out);
                    return 0;
                }
                ty_cache_dict_set(out, &key, &value);
            }
            ty_cache_slot_release(&key);
            ty_cache_slot_release(&value);
        }
        return 1;
    }
    return 0;
}

static void ty_cache_decode_sentinel(TyCacheSlot *out) {
    TyCacheSlot key;
    TyCacheSlot value;
    ty_cache_slot_init(&key);
    ty_cache_slot_init(&value);
    ty_cache_set_none(out);
    ty_cache_dict_new(out, 1);
    ty_cache_set_string(&key, "fingerprint", 11);
    ty_cache_set_string(&value, "", 0);
    ty_cache_dict_set(out, &key, &value);
    ty_cache_slot_release(&key);
    ty_cache_slot_release(&value);
}

int ty_cache_encode_export(TyCacheSlot *, const TyCacheSlot *) __asm__("__tyrion_app_cache_encode");
int ty_cache_encode_export(TyCacheSlot *out, const TyCacheSlot *value) {
    static const char header[] = "TYRION_AST_CACHE_V1\n";
    TyCacheBuffer buffer = {0};
    int ok = ty_cache_buffer_append(&buffer, header, sizeof(header) - 1) &&
             ty_cache_encode_value(value, &buffer, 0);
    if (!ok) {
        free(buffer.data);
        return ty_cache_set_string(out, "invalid-cache", 13);
    }
    int published = ty_cache_set_string(out, buffer.data, buffer.length);
    free(buffer.data);
    return published;
}

int ty_cache_decode_export(TyCacheSlot *, const TyCacheSlot *) __asm__("__tyrion_app_cache_decode");
int ty_cache_decode_export(TyCacheSlot *out, const TyCacheSlot *encoded) {
    static const char header[] = "TYRION_AST_CACHE_V1\n";
    const char *data = ty_cache_string_data(encoded);
    size_t length = ty_cache_string_length(encoded);
    if (!data || length < sizeof(header) - 1 || memcmp(data, header, sizeof(header) - 1) != 0) {
        ty_cache_decode_sentinel(out);
        return 1;
    }
    TyCacheReader reader = {data, length, sizeof(header) - 1};
    if (!ty_cache_decode_value(&reader, out, 0) || reader.position != reader.length) {
        ty_cache_decode_sentinel(out);
    }
    return 1;
}
