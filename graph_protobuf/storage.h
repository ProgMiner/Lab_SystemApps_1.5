#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>


typedef struct {
    int fd;
} storage;

struct storage_pointer {
    storage storage;
    uint64_t offset;
};

struct storage_vertex {
    struct storage_pointer pointer;
};

struct storage_edge {
    struct storage_pointer pointer;
};

struct storage_label {
    struct storage_pointer pointer;
};

struct storage_attribute {
    struct storage_pointer pointer;
};


// storage ops

bool storage_init(int fd, storage * storage);
bool storage_open(int fd, storage * storage);

bool storage_get_first_vertex(storage storage, struct storage_vertex * vertex);
bool storage_get_first_edge(storage storage, struct storage_edge * edge);

bool storage_create_vertex(storage storage, struct storage_vertex * vertex);
bool storage_create_edge(storage storage, struct storage_edge * edge);

// storage_vertex ops

bool storage_vertex_next(struct storage_vertex * vertex);

bool storage_vertex_has_label(struct storage_vertex vertex, const char * label);
bool storage_vertex_add_label(struct storage_vertex vertex, const char * label);
bool storage_vertex_remove_label(struct storage_vertex vertex, const char * label);

bool storage_vertex_get_labels_amount(struct storage_vertex vertex, size_t * amount);
bool storage_vertex_get_first_label(struct storage_vertex vertex, struct storage_label * label);

bool storage_vertex_get_attribute(struct storage_vertex vertex, const char * name, char ** value);
bool storage_vertex_set_attribute(struct storage_vertex vertex, const char * name, const char * value);
bool storage_vertex_remove_attribute(struct storage_vertex vertex, const char * name);

bool storage_vertex_get_attributes_amount(struct storage_vertex vertex, size_t * amount);
bool storage_vertex_get_first_attribute(struct storage_vertex vertex, struct storage_attribute * attribute);

bool storage_vertex_drop(struct storage_vertex vertex);

static bool storage_vertex_equals(struct storage_vertex a, struct storage_vertex b) {
    return a.pointer.offset == b.pointer.offset && a.pointer.storage.fd == b.pointer.storage.fd;
}

// storage_edge ops

bool storage_edge_next(struct storage_edge * edge);

bool storage_edge_get_source(struct storage_edge edge, struct storage_vertex * vertex);
bool storage_edge_set_source(struct storage_edge edge, struct storage_vertex vertex);

bool storage_edge_get_destination(struct storage_edge edge, struct storage_vertex * vertex);
bool storage_edge_set_destination(struct storage_edge edge, struct storage_vertex vertex);

bool storage_edge_get_label(struct storage_edge edge, char ** label);
bool storage_edge_set_label(struct storage_edge edge, const char * label);
bool storage_edge_remove_label(struct storage_edge edge, const char * label);

bool storage_edge_drop(struct storage_edge edge);

// storage_label ops

bool storage_label_next(struct storage_label * label);
bool storage_label_get(struct storage_label label, char ** value);

// storage_attribute ops

bool storage_attribute_next(struct storage_attribute * attribute);
bool storage_attribute_get(struct storage_attribute attribute, char ** name, char ** value);
