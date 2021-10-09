#pragma once

#include <stdint.h>
#include <stdbool.h>


#define STORAGE_SIGNATURE ("DOTA")
#define STORAGE_SIGNATURE_SIZE (4)


typedef uint64_t storage_pointer_;

struct storage_list_ {
    storage_pointer_ head;
    storage_pointer_ tail;
} __attribute__((packed));

struct storage_list_node_ {
    storage_pointer_ next;
    storage_pointer_ value;
} __attribute__((packed));

struct storage_string_ {
    char * value;
};

struct storage_attribute_ {
    struct storage_string_ name;
    storage_pointer_ value;
};

struct storage_vertex_ {
    struct storage_list_ labels;
    struct storage_list_ attributes;
} __attribute__((packed));

struct storage_edge_ {
    storage_pointer_ label;
    storage_pointer_ source;
    storage_pointer_ destination;
} __attribute__((packed));

struct storage_header_ {
    uint8_t signature[STORAGE_SIGNATURE_SIZE];
    struct storage_list_ vertices;
    struct storage_list_ edges;
} __attribute__((packed));


// storage_list_ ops

void storage_list__destroy(struct storage_list_ value);

bool storage_list__read(int fd, struct storage_list_ * value);
bool storage_list__write(int fd, const struct storage_list_ * value);

// storage_list_node_ ops

void storage_list_node__destroy(struct storage_list_node_ value);

bool storage_list_node__read(int fd, struct storage_list_node_ * value);
bool storage_list_node__write(int fd, const struct storage_list_node_ * value);

// storage_string_ ops

bool storage_string__init(const char * value, struct storage_string_ * string);
void storage_string__destroy(struct storage_string_ value);

bool storage_string__read(int fd, struct storage_string_ * value);
bool storage_string__write(int fd, const struct storage_string_ * value);

// storage_attribute_

void storage_attribute__destroy(struct storage_attribute_ value);

bool storage_attribute__read(int fd, struct storage_attribute_ * value);
bool storage_attribute__write(int fd, const struct storage_attribute_ * value);

// storage_vertex_ ops

void storage_vertex__destroy(struct storage_vertex_ value);

bool storage_vertex__read(int fd, struct storage_vertex_ * value);
bool storage_vertex__write(int fd, const struct storage_vertex_ * value);

// storage_edge_ ops

void storage_edge__destroy(struct storage_edge_ value);

bool storage_edge__read(int fd, struct storage_edge_ * value);
bool storage_edge__write(int fd, const struct storage_edge_ * value);

// storage_header_ ops

bool storage_header__init(struct storage_header_ * header);
void storage_header__destroy(struct storage_header_ value);

bool storage_header__read(int fd, struct storage_header_ * value);
bool storage_header__write(int fd, const struct storage_header_ * value);

bool storage_header__read_and_check(int fd, struct storage_header_ * header);
