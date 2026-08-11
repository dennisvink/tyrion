#include <ctype.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int seeded = 0;
static char out_buf[4096];

static void ensure_seeded(void) {
  if (!seeded) {
    struct timespec ts;
    uint64_t sec = 0;
    uint64_t nsec = 0;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
      sec = (uint64_t)ts.tv_sec;
      nsec = (uint64_t)ts.tv_nsec;
    } else {
      sec = (uint64_t)time(NULL);
    }
    uint64_t pid = (uint64_t)getpid();
    uint64_t addr = (uint64_t)(uintptr_t)&seeded;
    uint64_t seed = sec ^ nsec ^ pid ^ addr;
    seeded = 1;
    srand((unsigned int)(seed & 0xffffffffu));
  }
}

const char* tyext_foobar_call_v1(const char* op, const char* payload) {
  if (!op) {
    return "<none>";
  }
  if (strcmp(op, "foobar") != 0 && strcmp(op, "foobar.randomcase") != 0) {
    return "<none>";
  }
  if (!payload) {
    return "<none>";
  }

  ensure_seeded();
  size_t in_len = strlen(payload);
  if (in_len >= sizeof(out_buf)) {
    in_len = sizeof(out_buf) - 1;
  }

  for (size_t i = 0; i < in_len; i++) {
    unsigned char ch = (unsigned char)payload[i];
    if (isalpha(ch)) {
      out_buf[i] = (rand() & 1) ? (char)toupper(ch) : (char)tolower(ch);
    } else {
      out_buf[i] = (char)ch;
    }
  }
  out_buf[in_len] = '\0';
  return out_buf;
}
