#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "mbedtls/ctr_drbg.h"
#include "mbedtls/entropy.h"
#include "mbedtls/error.h"
#include "mbedtls/net_sockets.h"
#include "mbedtls/ssl.h"
#include "mbedtls/x509_crt.h"

#define TYRION_TLS_MAX_SESSIONS 32
#define TYRION_TLS_MAX_IO (1024 * 1024)
#define TYRION_TLS_MAX_HOST 253

struct tyrion_tls_session {
  int used;
  mbedtls_net_context net;
  mbedtls_entropy_context entropy;
  mbedtls_ctr_drbg_context rng;
  mbedtls_ssl_context ssl;
  mbedtls_ssl_config config;
  mbedtls_x509_crt roots;
};

static struct tyrion_tls_session tyrion_tls_sessions[TYRION_TLS_MAX_SESSIONS];
static char *tyrion_tls_result;
static size_t tyrion_tls_result_capacity;

static const char *tyrion_tls_set_result(const char *format, ...) {
  va_list args;
  va_list measure;
  int needed;

  va_start(args, format);
  va_copy(measure, args);
  needed = vsnprintf(NULL, 0, format, measure);
  va_end(measure);
  if (needed < 0) {
    va_end(args);
    return NULL;
  }
  if ((size_t)needed + 1 > tyrion_tls_result_capacity) {
    size_t capacity = (size_t)needed + 1;
    char *next = (char *)realloc(tyrion_tls_result, capacity);
    if (next == NULL) {
      va_end(args);
      return NULL;
    }
    tyrion_tls_result = next;
    tyrion_tls_result_capacity = capacity;
  }
  (void)vsnprintf(tyrion_tls_result, tyrion_tls_result_capacity, format, args);
  va_end(args);
  return tyrion_tls_result;
}

static const char *tyrion_tls_error(const char *operation, int code) {
  char detail[192];
  detail[0] = '\0';
  if (code != 0) {
    mbedtls_strerror(code, detail, sizeof(detail));
  }
  return tyrion_tls_set_result("err:%s:%d:%s", operation, code,
                               detail[0] == '\0' ? "failed" : detail);
}

static int tyrion_tls_parse_long(const char *text, long minimum, long maximum,
                                 long *value) {
  char *end = NULL;
  long parsed;
  if (text == NULL || *text == '\0') {
    return 0;
  }
  errno = 0;
  parsed = strtol(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed < minimum ||
      parsed > maximum) {
    return 0;
  }
  *value = parsed;
  return 1;
}

static int tyrion_tls_split3(char *text, char **first, char **second,
                             char **third) {
  char *a = strchr(text, '\n');
  char *b;
  if (a == NULL) {
    return 0;
  }
  *a = '\0';
  b = strchr(a + 1, '\n');
  if (b == NULL) {
    return 0;
  }
  *b = '\0';
  if (strchr(b + 1, '\n') != NULL) {
    return 0;
  }
  *first = text;
  *second = a + 1;
  *third = b + 1;
  return 1;
}

static void tyrion_tls_session_init(struct tyrion_tls_session *session) {
  memset(session, 0, sizeof(*session));
  mbedtls_net_init(&session->net);
  mbedtls_entropy_init(&session->entropy);
  mbedtls_ctr_drbg_init(&session->rng);
  mbedtls_ssl_init(&session->ssl);
  mbedtls_ssl_config_init(&session->config);
  mbedtls_x509_crt_init(&session->roots);
}

static void tyrion_tls_session_free(struct tyrion_tls_session *session) {
  if (session == NULL || !session->used) {
    return;
  }
  (void)mbedtls_ssl_close_notify(&session->ssl);
  mbedtls_net_free(&session->net);
  mbedtls_x509_crt_free(&session->roots);
  mbedtls_ssl_free(&session->ssl);
  mbedtls_ssl_config_free(&session->config);
  mbedtls_ctr_drbg_free(&session->rng);
  mbedtls_entropy_free(&session->entropy);
  memset(session, 0, sizeof(*session));
}

static struct tyrion_tls_session *tyrion_tls_session_at(long id) {
  if (id < 1 || id > TYRION_TLS_MAX_SESSIONS ||
      !tyrion_tls_sessions[id - 1].used) {
    return NULL;
  }
  return &tyrion_tls_sessions[id - 1];
}

static int tyrion_tls_connect_socket(const char *host, const char *port,
                                     int timeout_ms) {
  struct addrinfo hints;
  struct addrinfo *addresses = NULL;
  struct addrinfo *address;
  int descriptor = -1;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  if (getaddrinfo(host, port, &hints, &addresses) != 0) {
    return -1;
  }

  for (address = addresses; address != NULL; address = address->ai_next) {
    int flags;
    int result;
    descriptor = socket(address->ai_family, address->ai_socktype,
                        address->ai_protocol);
    if (descriptor < 0) {
      continue;
    }
    flags = fcntl(descriptor, F_GETFL, 0);
    if (flags < 0 || fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) < 0) {
      close(descriptor);
      descriptor = -1;
      continue;
    }
    result = connect(descriptor, address->ai_addr, address->ai_addrlen);
    if (result != 0 && errno == EINPROGRESS) {
      struct pollfd wait;
      int socket_error = 0;
      socklen_t socket_error_size = sizeof(socket_error);
      wait.fd = descriptor;
      wait.events = POLLOUT;
      wait.revents = 0;
      result = poll(&wait, 1, timeout_ms);
      if (result > 0 && (wait.revents & POLLOUT) != 0 &&
          getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &socket_error,
                     &socket_error_size) == 0 &&
          socket_error == 0) {
        result = 0;
      } else {
        result = -1;
      }
    }
    if (result == 0 && fcntl(descriptor, F_SETFL, flags) == 0) {
      struct timeval timeout;
      timeout.tv_sec = timeout_ms / 1000;
      timeout.tv_usec = (timeout_ms % 1000) * 1000;
      (void)setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout));
      (void)setsockopt(descriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                       sizeof(timeout));
      break;
    }
    close(descriptor);
    descriptor = -1;
  }
  freeaddrinfo(addresses);
  return descriptor;
}

static int tyrion_tls_load_roots(mbedtls_x509_crt *roots) {
  static const char *const candidates[] = {
      "/etc/ssl/cert.pem",
      "/etc/ssl/certs/ca-certificates.crt",
      "/etc/pki/tls/certs/ca-bundle.crt",
      "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
      NULL,
  };
  const char *configured = getenv("SSL_CERT_FILE");
  size_t index = 0;
  int result;
  if (configured != NULL && configured[0] != '\0') {
    result = mbedtls_x509_crt_parse_file(roots, configured);
    return result < 0 ? result : 0;
  }
  while (candidates[index] != NULL) {
    if (access(candidates[index], R_OK) == 0) {
      result = mbedtls_x509_crt_parse_file(roots, candidates[index]);
      if (result >= 0) {
        return 0;
      }
    }
    index++;
  }
  return MBEDTLS_ERR_X509_FILE_IO_ERROR;
}

static const char *tyrion_tls_connect(const char *payload) {
  static const unsigned char personalization[] = "tyrionic-tls-client-v1";
  static const char *alpn[] = {"http/1.1", NULL};
  char *copy;
  char *host;
  char *port;
  char *timeout_text;
  char port_service[6];
  long port_number;
  long timeout_ms;
  int slot = -1;
  int result;
  int index;
  struct tyrion_tls_session *session;

  copy = payload == NULL ? NULL : strdup(payload);
  if (copy == NULL) {
    return tyrion_tls_error("connect-payload", 0);
  }
  if (!tyrion_tls_split3(copy, &host, &port, &timeout_text) ||
      strlen(host) == 0 || strlen(host) > TYRION_TLS_MAX_HOST ||
      !tyrion_tls_parse_long(port, 1, 65535, &port_number)) {
    free(copy);
    return tyrion_tls_error("connect-payload", 0);
  }
  if (!tyrion_tls_parse_long(timeout_text, 1, 300000, &timeout_ms)) {
    free(copy);
    return tyrion_tls_error("connect-timeout", 0);
  }
  (void)snprintf(port_service, sizeof(port_service), "%ld", port_number);
  for (index = 0; index < TYRION_TLS_MAX_SESSIONS; index++) {
    if (!tyrion_tls_sessions[index].used) {
      slot = index;
      break;
    }
  }
  if (slot < 0) {
    free(copy);
    return tyrion_tls_error("connect-capacity", 0);
  }

  session = &tyrion_tls_sessions[slot];
  tyrion_tls_session_init(session);
  session->used = 1;
  session->net.fd =
      tyrion_tls_connect_socket(host, port_service, (int)timeout_ms);
  if (session->net.fd < 0) {
    free(copy);
    tyrion_tls_session_free(session);
    return tyrion_tls_error("tcp-connect", 0);
  }
  result = mbedtls_ctr_drbg_seed(&session->rng, mbedtls_entropy_func,
                                 &session->entropy, personalization,
                                 sizeof(personalization) - 1);
  if (result != 0) {
    free(copy);
    tyrion_tls_session_free(session);
    return tyrion_tls_error("rng", result);
  }
  result = tyrion_tls_load_roots(&session->roots);
  if (result != 0) {
    free(copy);
    tyrion_tls_session_free(session);
    return tyrion_tls_error("ca-roots", result);
  }
  result = mbedtls_ssl_config_defaults(&session->config, MBEDTLS_SSL_IS_CLIENT,
                                       MBEDTLS_SSL_TRANSPORT_STREAM,
                                       MBEDTLS_SSL_PRESET_DEFAULT);
  if (result != 0) {
    free(copy);
    tyrion_tls_session_free(session);
    return tyrion_tls_error("config", result);
  }
  mbedtls_ssl_conf_authmode(&session->config, MBEDTLS_SSL_VERIFY_REQUIRED);
  mbedtls_ssl_conf_ca_chain(&session->config, &session->roots, NULL);
  mbedtls_ssl_conf_rng(&session->config, mbedtls_ctr_drbg_random,
                       &session->rng);
  mbedtls_ssl_conf_read_timeout(&session->config, (uint32_t)timeout_ms);
  result = mbedtls_ssl_conf_alpn_protocols(&session->config, alpn);
  if (result != 0) {
    free(copy);
    tyrion_tls_session_free(session);
    return tyrion_tls_error("alpn", result);
  }
  result = mbedtls_ssl_setup(&session->ssl, &session->config);
  if (result == 0) {
    result = mbedtls_ssl_set_hostname(&session->ssl, host);
  }
  if (result != 0) {
    free(copy);
    tyrion_tls_session_free(session);
    return tyrion_tls_error("setup", result);
  }
  mbedtls_ssl_set_bio(&session->ssl, &session->net, mbedtls_net_send,
                      mbedtls_net_recv, mbedtls_net_recv_timeout);
  do {
    result = mbedtls_ssl_handshake(&session->ssl);
  } while (result == MBEDTLS_ERR_SSL_WANT_READ ||
           result == MBEDTLS_ERR_SSL_WANT_WRITE);
  free(copy);
  if (result != 0) {
    tyrion_tls_session_free(session);
    return tyrion_tls_error("handshake", result);
  }
  if (mbedtls_ssl_get_verify_result(&session->ssl) != 0) {
    tyrion_tls_session_free(session);
    return tyrion_tls_error("certificate", MBEDTLS_ERR_X509_CERT_VERIFY_FAILED);
  }
  return tyrion_tls_set_result("ok:%d", slot + 1);
}

static int tyrion_tls_hex_value(char value) {
  if (value >= '0' && value <= '9') {
    return value - '0';
  }
  if (value >= 'a' && value <= 'f') {
    return value - 'a' + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return value - 'A' + 10;
  }
  return -1;
}

static const char *tyrion_tls_write(const char *payload) {
  char *copy = payload == NULL ? NULL : strdup(payload);
  char *id_text;
  char *timeout_text;
  char *hex;
  long id;
  long timeout_ms;
  size_t length;
  unsigned char *bytes;
  size_t index;
  size_t offset = 0;
  struct tyrion_tls_session *session;
  int result;

  if (copy == NULL || !tyrion_tls_split3(copy, &id_text, &timeout_text, &hex) ||
      !tyrion_tls_parse_long(id_text, 1, TYRION_TLS_MAX_SESSIONS, &id) ||
      !tyrion_tls_parse_long(timeout_text, 1, 300000, &timeout_ms)) {
    free(copy);
    return tyrion_tls_error("write-payload", 0);
  }
  session = tyrion_tls_session_at(id);
  length = strlen(hex);
  if (session == NULL || length % 2 != 0 || length / 2 > TYRION_TLS_MAX_IO) {
    free(copy);
    return tyrion_tls_error("write-payload", 0);
  }
  bytes = (unsigned char *)malloc(length / 2 == 0 ? 1 : length / 2);
  if (bytes == NULL) {
    free(copy);
    return tyrion_tls_error("write-memory", 0);
  }
  for (index = 0; index < length; index += 2) {
    int high = tyrion_tls_hex_value(hex[index]);
    int low = tyrion_tls_hex_value(hex[index + 1]);
    if (high < 0 || low < 0) {
      free(bytes);
      free(copy);
      return tyrion_tls_error("write-hex", 0);
    }
    bytes[index / 2] = (unsigned char)((high << 4) | low);
  }
  mbedtls_ssl_conf_read_timeout(&session->config, (uint32_t)timeout_ms);
  while (offset < length / 2) {
    result = mbedtls_ssl_write(&session->ssl, bytes + offset,
                               length / 2 - offset);
    if (result == MBEDTLS_ERR_SSL_WANT_READ ||
        result == MBEDTLS_ERR_SSL_WANT_WRITE) {
      continue;
    }
    if (result <= 0) {
      free(bytes);
      free(copy);
      return tyrion_tls_error("write", result);
    }
    offset += (size_t)result;
  }
  free(bytes);
  free(copy);
  return tyrion_tls_set_result("ok:%zu", offset);
}

static const char *tyrion_tls_read(const char *payload) {
  static const char digits[] = "0123456789abcdef";
  char *copy = payload == NULL ? NULL : strdup(payload);
  char *id_text;
  char *maximum_text;
  char *timeout_text;
  long id;
  long maximum;
  long timeout_ms;
  struct tyrion_tls_session *session;
  unsigned char *bytes;
  int result;
  size_t index;

  if (copy == NULL ||
      !tyrion_tls_split3(copy, &id_text, &maximum_text, &timeout_text) ||
      !tyrion_tls_parse_long(id_text, 1, TYRION_TLS_MAX_SESSIONS, &id) ||
      !tyrion_tls_parse_long(maximum_text, 1, TYRION_TLS_MAX_IO, &maximum) ||
      !tyrion_tls_parse_long(timeout_text, 1, 300000, &timeout_ms)) {
    free(copy);
    return tyrion_tls_error("read-payload", 0);
  }
  session = tyrion_tls_session_at(id);
  if (session == NULL) {
    free(copy);
    return tyrion_tls_error("read-session", 0);
  }
  bytes = (unsigned char *)malloc((size_t)maximum);
  if (bytes == NULL) {
    free(copy);
    return tyrion_tls_error("read-memory", 0);
  }
  mbedtls_ssl_conf_read_timeout(&session->config, (uint32_t)timeout_ms);
  do {
    result = mbedtls_ssl_read(&session->ssl, bytes, (size_t)maximum);
  } while (result == MBEDTLS_ERR_SSL_WANT_READ ||
           result == MBEDTLS_ERR_SSL_WANT_WRITE);
  free(copy);
  if (result == 0 || result == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY) {
    free(bytes);
    return tyrion_tls_set_result("eof");
  }
  if (result < 0) {
    free(bytes);
    return tyrion_tls_error("read", result);
  }
  if ((size_t)result * 2 + 6 > tyrion_tls_result_capacity) {
    size_t capacity = (size_t)result * 2 + 6;
    char *next = (char *)realloc(tyrion_tls_result, capacity);
    if (next == NULL) {
      free(bytes);
      return NULL;
    }
    tyrion_tls_result = next;
    tyrion_tls_result_capacity = capacity;
  }
  memcpy(tyrion_tls_result, "data:", 5);
  for (index = 0; index < (size_t)result; index++) {
    tyrion_tls_result[5 + index * 2] = digits[bytes[index] >> 4];
    tyrion_tls_result[6 + index * 2] = digits[bytes[index] & 15];
  }
  tyrion_tls_result[5 + (size_t)result * 2] = '\0';
  free(bytes);
  return tyrion_tls_result;
}

static const char *tyrion_tls_close(const char *payload) {
  long id;
  struct tyrion_tls_session *session;
  if (!tyrion_tls_parse_long(payload, 1, TYRION_TLS_MAX_SESSIONS, &id)) {
    return tyrion_tls_error("close-payload", 0);
  }
  session = tyrion_tls_session_at(id);
  if (session == NULL) {
    return tyrion_tls_error("close-session", 0);
  }
  tyrion_tls_session_free(session);
  return tyrion_tls_set_result("ok");
}

int64_t tyext_tls_write_bytes_v1(int64_t id, const unsigned char *bytes,
                                 size_t length, int64_t timeout_ms) {
  struct tyrion_tls_session *session;
  size_t offset = 0;
  int result;

  if (id < 1 || id > TYRION_TLS_MAX_SESSIONS ||
      (bytes == NULL && length != 0) || length > TYRION_TLS_MAX_IO ||
      timeout_ms < 1 || timeout_ms > 300000) {
    return -1;
  }
  session = tyrion_tls_session_at((long)id);
  if (session == NULL) {
    return -1;
  }
  mbedtls_ssl_conf_read_timeout(&session->config, (uint32_t)timeout_ms);
  while (offset < length) {
    result = mbedtls_ssl_write(&session->ssl, bytes + offset, length - offset);
    if (result == MBEDTLS_ERR_SSL_WANT_READ ||
        result == MBEDTLS_ERR_SSL_WANT_WRITE) {
      continue;
    }
    if (result <= 0) {
      return -1;
    }
    offset += (size_t)result;
  }
  return (int64_t)offset;
}

int64_t tyext_tls_read_bytes_v1(int64_t id, unsigned char *bytes,
                                size_t maximum, int64_t timeout_ms) {
  struct tyrion_tls_session *session;
  int result;

  if (id < 1 || id > TYRION_TLS_MAX_SESSIONS || bytes == NULL ||
      maximum < 1 || maximum > TYRION_TLS_MAX_IO || timeout_ms < 1 ||
      timeout_ms > 300000) {
    return -1;
  }
  session = tyrion_tls_session_at((long)id);
  if (session == NULL) {
    return -1;
  }
  mbedtls_ssl_conf_read_timeout(&session->config, (uint32_t)timeout_ms);
  do {
    result = mbedtls_ssl_read(&session->ssl, bytes, maximum);
  } while (result == MBEDTLS_ERR_SSL_WANT_READ ||
           result == MBEDTLS_ERR_SSL_WANT_WRITE);
  if (result == 0 || result == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY) {
    return 0;
  }
  return result < 0 ? -1 : (int64_t)result;
}

const char *tyext_tls_call_v1(const char *operation, const char *payload) {
  if (operation == NULL || payload == NULL) {
    return tyrion_tls_error("call", 0);
  }
  if (strcmp(operation, "tls.connect") == 0) {
    return tyrion_tls_connect(payload);
  }
  if (strcmp(operation, "tls.write") == 0) {
    return tyrion_tls_write(payload);
  }
  if (strcmp(operation, "tls.read") == 0) {
    return tyrion_tls_read(payload);
  }
  if (strcmp(operation, "tls.close") == 0) {
    return tyrion_tls_close(payload);
  }
  return tyrion_tls_error("unknown-operation", 0);
}
