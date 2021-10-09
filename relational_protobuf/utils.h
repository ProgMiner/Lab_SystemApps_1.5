#pragma once

#include <stdbool.h>
#include <stddef.h>


#define STRINGIFY(_x) #_x
#define STRINGIFY_VALUE(_x) STRINGIFY(_x)


bool read_full(int fd, void * buf, size_t size);
bool write_full(int fd, const void * buf, size_t size);
