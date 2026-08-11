#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__linux__)
#include <fcntl.h>
#include <sys/random.h>
#include <unistd.h>
#endif

#define RAND_MAX_BYTES_HEX 4096

static char out_small[128];
static char out_hex[(RAND_MAX_BYTES_HEX * 2) + 1];
static unsigned char byte_buf[RAND_MAX_BYTES_HEX];

static int parse_u64(const char* s, uint64_t* out) {
  char* end = NULL;
  unsigned long long value = 0ULL;
  if (!s || !*s || !out) {
    return 0;
  }
  errno = 0;
  value = strtoull(s, &end, 10);
  if (errno != 0 || end == s || *end != '\0') {
    return 0;
  }
  *out = (uint64_t)value;
  return 1;
}

static int fill_from_urandom(unsigned char* dst, size_t n) {
#if defined(__linux__)
  int fd = open("/dev/urandom", O_RDONLY);
  if (fd < 0) {
    return 0;
  }
  size_t off = 0;
  while (off < n) {
    ssize_t got = read(fd, dst + off, n - off);
    if (got > 0) {
      off += (size_t)got;
      continue;
    }
    if (got < 0 && errno == EINTR) {
      continue;
    }
    close(fd);
    return 0;
  }
  close(fd);
  return 1;
#else
  FILE* f = fopen("/dev/urandom", "rb");
  size_t got = 0;
  if (!f) {
    return 0;
  }
  got = fread(dst, 1, n, f);
  fclose(f);
  return got == n;
#endif
}

static int secure_random_bytes(unsigned char* dst, size_t n) {
  if (!dst) {
    return 0;
  }
  if (n == 0) {
    return 1;
  }

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
  arc4random_buf(dst, n);
  return 1;
#elif defined(__linux__)
  size_t off = 0;
  while (off < n) {
    ssize_t got = getrandom(dst + off, n - off, 0);
    if (got > 0) {
      off += (size_t)got;
      continue;
    }
    if (got < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
  if (off == n) {
    return 1;
  }
  return fill_from_urandom(dst + off, n - off);
#else
  return fill_from_urandom(dst, n);
#endif
}

static int random_u64(uint64_t* out) {
  return secure_random_bytes((unsigned char*)out, sizeof(uint64_t));
}

static int random_below(uint64_t max_exclusive, uint64_t* out) {
  uint64_t x = 0;
  uint64_t threshold = 0;
  if (!out || max_exclusive == 0) {
    return 0;
  }
  threshold = (uint64_t)(0 - max_exclusive) % max_exclusive;
  for (;;) {
    if (!random_u64(&x)) {
      return 0;
    }
    if (x >= threshold) {
      *out = x % max_exclusive;
      return 1;
    }
  }
}

static int starts_with(const char* text, const char* prefix) {
  size_t i = 0;
  if (!text || !prefix) {
    return 0;
  }
  while (prefix[i] != '\0') {
    if (text[i] != prefix[i]) {
      return 0;
    }
    i++;
  }
  return 1;
}

const char* tyext_rand_call_v1(const char* op, const char* payload) {
  uint64_t value = 0;
  uint64_t arg = 0;
  double f = 0.0;
  static const char hex[] = "0123456789abcdef";
  size_t i = 0;

  if (!op) {
    return NULL;
  }

  if (strcmp(op, "rand.exec") == 0) {
    if (!payload) {
      return NULL;
    }
    if (strcmp(payload, "float") == 0) {
      op = "rand.float";
      payload = "";
    } else if (strcmp(payload, "u64") == 0) {
      op = "rand.u64";
      payload = "";
    } else if (strcmp(payload, "bool") == 0) {
      op = "rand.bool";
      payload = "";
    } else if (starts_with(payload, "int:")) {
      op = "rand.int";
      payload = payload + 4;
    } else if (starts_with(payload, "hex:")) {
      op = "rand.bytes.hex";
      payload = payload + 4;
    } else {
      return NULL;
    }
  }

  if (strcmp(op, "rand") == 0 || strcmp(op, "rand.float") == 0) {
    if (!random_u64(&value)) {
      return NULL;
    }
    value >>= 11; /* 53 random bits */
    f = (double)value / 9007199254740992.0; /* 2^53 */
    snprintf(out_small, sizeof(out_small), "%.17g", f);
    return out_small;
  }

  if (strcmp(op, "rand.u64") == 0) {
    if (!random_u64(&value)) {
      return NULL;
    }
    value &= (uint64_t)INT64_MAX;
    snprintf(out_small, sizeof(out_small), "%llu", (unsigned long long)value);
    return out_small;
  }

  if (strcmp(op, "rand.int") == 0) {
    if (!parse_u64(payload, &arg) || arg == 0) {
      return NULL;
    }
    if (!random_below(arg, &value)) {
      return NULL;
    }
    snprintf(out_small, sizeof(out_small), "%llu", (unsigned long long)value);
    return out_small;
  }

  if (strcmp(op, "rand.bool") == 0) {
    if (!random_below(2, &value)) {
      return NULL;
    }
    return value ? "1" : "0";
  }

  if (strcmp(op, "rand.bytes.hex") == 0) {
    if (!parse_u64(payload, &arg) || arg > RAND_MAX_BYTES_HEX) {
      return NULL;
    }
    if (!secure_random_bytes(byte_buf, (size_t)arg)) {
      return NULL;
    }
    for (i = 0; i < (size_t)arg; i++) {
      unsigned char b = byte_buf[i];
      out_hex[(i * 2)] = hex[(b >> 4) & 0x0f];
      out_hex[(i * 2) + 1] = hex[b & 0x0f];
    }
    out_hex[(size_t)arg * 2] = '\0';
    return out_hex;
  }

  return NULL;
}
