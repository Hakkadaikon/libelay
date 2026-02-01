#include "db_file.h"

#include "../../arch/close.h"
#include "../../arch/fstat.h"
#include "../../arch/fsync.h"
#include "../../arch/open.h"
#include "../../util/string.h"

bool nostr_db_file_exists(const char* path)
{
  require_not_null(path, false);

  int32_t fd = internal_open(path, O_RDONLY, 0);
  if (fd < 0) {
    return false;
  }

  internal_close(fd);
  return true;
}

int32_t nostr_db_file_create(const char* path, size_t initial_size)
{
  require_not_null(path, -1);

  // Create file with read-write permissions for owner
  int32_t fd = internal_open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    return -1;
  }

  // Set initial size
  if (initial_size > 0) {
    if (internal_ftruncate(fd, (int64_t)initial_size) < 0) {
      internal_close(fd);
      return -1;
    }
  }

  return fd;
}

int32_t nostr_db_file_open(const char* path, bool writable)
{
  require_not_null(path, -1);

  int32_t flags = writable ? O_RDWR : O_RDONLY;
  return internal_open(path, flags, 0);
}

int32_t nostr_db_file_close(int32_t fd)
{
  require(fd >= 0, -1);
  return internal_close(fd);
}

int64_t nostr_db_file_get_size(int32_t fd)
{
  require(fd >= 0, -1);

  LinuxStat stat;
  if (internal_fstat(fd, &stat) < 0) {
    return -1;
  }

  return stat.st_size;
}

int32_t nostr_db_file_extend(int32_t fd, size_t new_size)
{
  require(fd >= 0, -1);
  return internal_ftruncate(fd, (int64_t)new_size);
}

int32_t nostr_db_file_sync(int32_t fd)
{
  require(fd >= 0, -1);
  return internal_fsync(fd);
}
