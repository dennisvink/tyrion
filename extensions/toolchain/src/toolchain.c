#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern const unsigned char tyrion_toolchain_tls_start[];
extern const unsigned char tyrion_toolchain_tls_end[];

static char tyrion_toolchain_result[256];

static const char *tyrion_toolchain_error(const char *operation) {
  (void)snprintf(tyrion_toolchain_result, sizeof(tyrion_toolchain_result),
                 "err:%s:%d", operation, errno);
  return tyrion_toolchain_result;
}

static int tyrion_toolchain_resource(const char *name,
                                     const unsigned char **start,
                                     size_t *size) {
  if (name != NULL && strcmp(name, "tls") == 0) {
    *start = tyrion_toolchain_tls_start;
    *size = (size_t)(tyrion_toolchain_tls_end - tyrion_toolchain_tls_start);
    return 1;
  }
  return 0;
}

static const char *tyrion_toolchain_write(const char *payload) {
  const unsigned char *start;
  size_t size;
  const char *separator;
  const char *path;
  char name[65];
  size_t name_size;
  FILE *output;
  int write_failed;
  int close_failed;

  separator = payload == NULL ? NULL : strchr(payload, '\n');
  if (separator == NULL || strchr(separator + 1, '\n') != NULL) {
    errno = EINVAL;
    return tyrion_toolchain_error("resource-write-payload");
  }
  name_size = (size_t)(separator - payload);
  if (name_size == 0 || name_size >= sizeof(name)) {
    errno = EINVAL;
    return tyrion_toolchain_error("resource-write-name");
  }
  memcpy(name, payload, name_size);
  name[name_size] = '\0';
  path = separator + 1;
  if (*path == '\0' || !tyrion_toolchain_resource(name, &start, &size)) {
    errno = ENOENT;
    return tyrion_toolchain_error("resource-write-missing");
  }
  output = fopen(path, "wb");
  if (output == NULL) {
    return tyrion_toolchain_error("resource-write-open");
  }
  write_failed = size > 0 && fwrite(start, 1, size, output) != size;
  close_failed = fclose(output) != 0;
  if (write_failed || close_failed) {
    return tyrion_toolchain_error("resource-write-io");
  }
  return "ok";
}

const char *tyext_toolchain_call_v1(const char *operation,
                                    const char *payload) {
  const unsigned char *start;
  size_t size;
  if (operation == NULL || payload == NULL) {
    errno = EINVAL;
    return tyrion_toolchain_error("call");
  }
  if (strcmp(operation, "resource.has") == 0) {
    if (!tyrion_toolchain_resource(payload, &start, &size)) {
      return "0";
    }
    return "1";
  }
  if (strcmp(operation, "resource.size") == 0) {
    if (!tyrion_toolchain_resource(payload, &start, &size)) {
      errno = ENOENT;
      return tyrion_toolchain_error("resource-size-missing");
    }
    (void)snprintf(tyrion_toolchain_result, sizeof(tyrion_toolchain_result),
                   "%zu", size);
    return tyrion_toolchain_result;
  }
  if (strcmp(operation, "resource.write") == 0) {
    return tyrion_toolchain_write(payload);
  }
  errno = ENOSYS;
  return tyrion_toolchain_error("unknown-operation");
}

#ifdef TYRION_DYNAMIC_EXTENSION
#ifndef TYRION_DYNAMIC_MANIFEST_ABI_MAJOR
#define TYRION_DYNAMIC_MANIFEST_ABI_MAJOR 1
#endif
#ifndef TYRION_DYNAMIC_MANIFEST_NAME
#define TYRION_DYNAMIC_MANIFEST_NAME "toolchain"
#endif
#ifndef TYRION_DYNAMIC_MANIFEST_CALL_SYMBOL
#define TYRION_DYNAMIC_MANIFEST_CALL_SYMBOL "tyext_toolchain_call_v1"
#endif

struct tyrion_host_extension_manifest_v1 {
  uint64_t size;
  uint64_t abi_major;
  const char *name;
  const char *call_symbol;
};

const struct tyrion_host_extension_manifest_v1 *
tyrion_host_extension_manifest_v1(void) {
  static const struct tyrion_host_extension_manifest_v1 manifest = {
      sizeof(struct tyrion_host_extension_manifest_v1),
      TYRION_DYNAMIC_MANIFEST_ABI_MAJOR,
      TYRION_DYNAMIC_MANIFEST_NAME,
      TYRION_DYNAMIC_MANIFEST_CALL_SYMBOL,
  };
  return &manifest;
}
#endif
