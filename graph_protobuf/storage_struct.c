#include "storage_struct.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "utils.h"


void storage_list__destroy(struct storage_list_ value) {
    // do nothing
}

bool storage_list__read(int fd, struct storage_list_ * value) {
    return read_full_value(fd, value);
}

bool storage_list__write(int fd, const struct storage_list_ * value) {
    return write_full_value(fd, value);
}

void storage_list_node__destroy(struct storage_list_node_ value) {
    // do nothing
}

bool storage_list_node__read(int fd, struct storage_list_node_ * value) {
    return read_full_value(fd, value);
}

bool storage_list_node__write(int fd, const struct storage_list_node_ * value) {
    return write_full_value(fd, value);
}

bool storage_string__init(const char * value, struct storage_string_ * string) {
    if (!value) {
        errno = EINVAL;
        return false;
    }

    struct storage_string_ new_string = { 0 };

    new_string.value = strdup(value);
    if (!new_string.value) {
        return false;
    }

    *string = new_string;
    return true;
}

void storage_string__destroy(struct storage_string_ value) {
    free(value.value);
}

bool storage_string__read(int fd, struct storage_string_ * value) {
    if (!value) {
        errno = EINVAL;
        return false;
    }

    uint64_t length;
    if (!read_full_value(fd, &length)) {
        return false;
    }

    value->value = malloc(sizeof(char) * (length + 1));
    if (!value->value) {
        return false;
    }

    if (!read_full(fd, value->value, length)) {
        return false;
    }

    value->value[length] = '\0';
    return true;
}

bool storage_string__write(int fd, const struct storage_string_ * value) {
    if (!value || !value->value) {
        errno = EINVAL;
        return false;
    }

    const uint64_t length = strlen(value->value);
    if (!write_full_value(fd, &length)) {
        return false;
    }

    if (!write_full(fd, value->value, length)) {
        return false;
    }

    return true;
}

void storage_attribute__destroy(struct storage_attribute_ value) {
    storage_string__destroy(value.name);
}

bool storage_attribute__read(int fd, struct storage_attribute_ * value) {
    if (!value) {
        errno = EINVAL;
        return false;
    }

    if (!storage_string__read(fd, &(value->name))) {
        return false;
    }

    if (!read_full_value(fd, &(value->value))) {
        return false;
    }

    return true;
}

bool storage_attribute__write(int fd, const struct storage_attribute_ * value) {
    if (!value) {
        errno = EINVAL;
        return false;
    }

    if (!storage_string__write(fd, &(value->name))) {
        return false;
    }

    if (!write_full_value(fd, &(value->value))) {
        return false;
    }

    return true;
}

void storage_vertex__destroy(struct storage_vertex_ value) {
    // do nothing
}

bool storage_vertex__read(int fd, struct storage_vertex_ * value) {
    return read_full_value(fd, value);
}

bool storage_vertex__write(int fd, const struct storage_vertex_ * value) {
    return write_full_value(fd, value);
}

void storage_edge__destroy(struct storage_edge_ value) {
    // do nothing
}

bool storage_edge__read(int fd, struct storage_edge_ * value) {
    return read_full_value(fd, value);
}

bool storage_edge__write(int fd, const struct storage_edge_ * value) {
    return write_full_value(fd, value);
}

bool storage_header__init(struct storage_header_ * header) {
    struct storage_header_ new_header = { 0 };

    memcpy(new_header.signature, STORAGE_SIGNATURE, STORAGE_SIGNATURE_SIZE);

    *header = new_header;
    return true;
}

void storage_header__destroy(struct storage_header_ value) {
    // do nothing
}

bool storage_header__read(int fd, struct storage_header_ * value) {
    return read_full_value(fd, value);
}

bool storage_header__write(int fd, const struct storage_header_ * value) {
    return write_full_value(fd, value);
}

bool storage_header__read_and_check(int fd, struct storage_header_ * header) {
    if (!storage_header__read(fd, header)) {
        return false;
    }

    if (memcmp(header->signature, STORAGE_SIGNATURE, STORAGE_SIGNATURE_SIZE) != 0) {
        errno = EINVAL;
        return false;
    }

    return true;
}
