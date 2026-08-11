#include <string.h>
#include <stdint.h>

const char* tyext_foo_call_v1(const char* op, const char* payload) {
  if (op && strcmp(op, "foo.echo") == 0) {
    return payload;
  }
  if (op && strcmp(op, "foo.ping") == 0) {
    return "pong";
  }
  return NULL;
}

#ifdef TYRION_DYNAMIC_EXTENSION
#ifndef TYRION_DYNAMIC_MANIFEST_ABI_MAJOR
#define TYRION_DYNAMIC_MANIFEST_ABI_MAJOR 1
#endif
#ifndef TYRION_DYNAMIC_MANIFEST_NAME
#define TYRION_DYNAMIC_MANIFEST_NAME "foo"
#endif
#ifndef TYRION_DYNAMIC_MANIFEST_CALL_SYMBOL
#define TYRION_DYNAMIC_MANIFEST_CALL_SYMBOL "tyext_foo_call_v1"
#endif

struct tyrion_host_extension_manifest_v1 {
  uint64_t size;
  uint64_t abi_major;
  const char* name;
  const char* call_symbol;
};

const struct tyrion_host_extension_manifest_v1*
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
