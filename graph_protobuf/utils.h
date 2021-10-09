#pragma once

#include <stdbool.h>
#include <stddef.h>


#define STRINGIFY(_x) #_x
#define STRINGIFY_VALUE(_x) STRINGIFY(_x)

#define read_full_value(_fd, _buf) read_full(_fd, _buf, sizeof(*_buf))
#define write_full_value(_fd, _buf) write_full(_fd, _buf, sizeof(*_buf))


bool read_full(int fd, void * buf, size_t size);
bool write_full(int fd, const void * buf, size_t size);

size_t strlen_utf8(const char * str);
