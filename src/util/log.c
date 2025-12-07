#include "log.h"

#include "../arch/write.h"
#include "./string.h"

static inline void hex_dump_char(char c)
{
  unsigned char uc           = (unsigned char)c;
  char          hex_digits[] = "0123456789ABCDEF";
  char          buf[3];

  buf[0] = hex_digits[uc / 16];
  buf[1] = hex_digits[uc % 16];
  buf[2] = ' ';

  internal_write(STDOUT_FILENO, buf, 3);
}

void hex_dump_local(const void* restrict data, size_t size)
{
  const char* byte_data = (const char*)data;

  for (size_t i = 0; i < size; i++) {
    hex_dump_char(byte_data[i]);

    if ((i + 1) % 16 == 0) {
      internal_write(STDOUT_FILENO, "\n", 1);
    }
  }

  if (size % 16 != 0) {
    internal_write(STDOUT_FILENO, "\n", 1);
  }
}

void log_dump_local(const int32_t fd, const char* restrict str)
{
  if (is_null(str) || fd <= 0) {
    return;
  }

  size_t len = strlen(str);
  if (len == 0) {
    return;
  }

  (void)internal_write(fd, str, len);
}

void var_dump_local(const int32_t fd, const char* restrict str, const int32_t value)
{
  if (is_null(str) || fd <= 0) {
    return;
  }

  size_t len = strlen(str);
  if (len == 0) {
    return;
  }

  (void)internal_write(fd, str, len);

  char   buffer[32];
  size_t buffer_size      = itoa(value, buffer, sizeof(buffer));
  buffer[buffer_size]     = '\n';
  buffer[buffer_size + 1] = '\0';

  (void)internal_write(fd, buffer, buffer_size + 1);
}

void str_dump_local(const int32_t fd, const char* restrict str, const char* restrict value)
{
  if (is_null(str) || is_null(value) || fd <= 0) {
    return;
  }

  log_dump(fd, str);
  log_dump(fd, value);
  log_dump(fd, "\n");
}
