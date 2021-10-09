#include "utils.h"

#include <stdint.h>
#include <unistd.h>
#include <errno.h>


bool read_full(int fd, void * buf, size_t size) {
    if (!buf) {
        errno = EINVAL;
        return false;
    }

    uint8_t * ptr = buf;
    ssize_t bytes_read;

    errno = 0;
    while ((bytes_read = read(fd, ptr, size)) > 0) {
        size -= bytes_read;
        ptr += bytes_read;

        if (size == 0) {
            return true;
        }
    }

    if (bytes_read == 0 && errno == 0) {
        errno = EPIPE;
    }

    return false;
}

bool write_full(int fd, const void * buf, size_t size) {
    const uint8_t * ptr = buf;
    ssize_t wrote;

    errno = 0;
    while ((wrote = write(fd, ptr, size)) >= 0) {
        size -= wrote;
        ptr += wrote;

        if (size == 0) {
            return true;
        }
    }

    return false;
}
