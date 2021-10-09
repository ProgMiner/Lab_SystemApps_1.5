#include "utils.h"

#include <stdint.h>
#include <unistd.h>
#include <wchar.h>
#include <errno.h>
#include <string.h>


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

size_t strlen_utf8(const char * str) {
    const size_t real_strlen = strlen(str);

    const char * ptr = str;
    mbstate_t mbs = { 0 };

    size_t c = 0;
    for (size_t remaining = real_strlen; remaining > 0; ++c) {
        const size_t len = mbrlen(ptr, remaining, &mbs);

        if (len > remaining) {
            return real_strlen;
        }

        remaining -= len;
    }

    return c;
}
