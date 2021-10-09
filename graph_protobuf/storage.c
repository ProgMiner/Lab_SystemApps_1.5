#define _LARGEFILE64_SOURCE

#include "storage.h"

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "storage_struct.h"
#include "utils.h"


#define POINTER_TO_CHILD(_offset, _type, _member) ((_offset) + offsetof(_type, _member))


static bool go_to_offset(int fd, storage_pointer_ offset) {
    return lseek64(fd, (off64_t) offset, SEEK_SET) != (off64_t) -1;
}

static bool go_to_end(int fd, storage_pointer_ * offset) {
    const off64_t off = lseek64(fd, 0, SEEK_END);

    if (off == (off64_t) -1) {
        return false;
    }

    if (offset) {
        *offset = (storage_pointer_) off;
    }

    return true;
}

static bool get_next_list_node_pointer(struct storage_pointer * current) {
    if (!current) {
        errno = EINVAL;
        return false;
    }

    const int fd = current->storage.fd;

    if (!go_to_offset(fd, current->offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        storage_list_node__destroy(node);
        return false;
    }

    current->offset = node.next;

    storage_list_node__destroy(node);
    return true;
}

static bool add_element_to_list(int fd, storage_pointer_ list, storage_pointer_ value, storage_pointer_ * node) {
    if (!go_to_offset(fd, list)) {
        return false;
    }

    struct storage_list_ storage_list;
    if (!storage_list__read(fd, &storage_list)) {
        return false;
    }

    storage_pointer_ new_node_ptr;
    if (!go_to_end(fd, &new_node_ptr)) {
        storage_list__destroy(storage_list);
        return false;
    }

    {
        struct storage_list_node_ new_node = {.next = 0, .value = value};
        if (!storage_list_node__write(fd, &new_node)) {
            storage_list_node__destroy(new_node);
            storage_list__destroy(storage_list);
            return false;
        }

        storage_list_node__destroy(new_node);
    }

    if (storage_list.tail) {
        if (!go_to_offset(fd, storage_list.tail)) {
            storage_list__destroy(storage_list);
            return false;
        }

        struct storage_list_node_ tail;
        if (!storage_list_node__read(fd, &tail)) {
            storage_list__destroy(storage_list);
            return false;
        }

        tail.next = new_node_ptr;

        if (!go_to_offset(fd, storage_list.tail)) {
            storage_list_node__destroy(tail);
            storage_list__destroy(storage_list);
            return false;
        }

        if (!storage_list_node__write(fd, &tail)) {
            storage_list_node__destroy(tail);
            storage_list__destroy(storage_list);
            return false;
        }

        storage_list_node__destroy(tail);
    } else {
        storage_list.head = new_node_ptr;
    }

    storage_list.tail = new_node_ptr;

    if (!go_to_offset(fd, list)) {
        storage_list__destroy(storage_list);
        return false;
    }

    if (!storage_list__write(fd, &storage_list)) {
        storage_list__destroy(storage_list);
        return false;
    }

    if (node) {
        *node = new_node_ptr;
    }

    storage_list__destroy(storage_list);
    return true;
}

static bool remove_node_from_list(int fd, storage_pointer_ list, storage_pointer_ node) {
    if (!go_to_offset(fd, list)) {
        return false;
    }

    struct storage_list_ storage_list;
    if (!storage_list__read(fd, &storage_list)) {
        return false;
    }

    // empty list
    if (storage_list.head == 0 && storage_list.tail == 0) {
        storage_list__destroy(storage_list);
        return true;
    }

    // node is head of list
    if (storage_list.head == node) {
        // list has one element - node
        if (storage_list.tail == node) {
            storage_list.head = 0;
            storage_list.tail = 0;
        } else {
            if (!go_to_offset(fd, node)) {
                storage_list__destroy(storage_list);
                return false;
            }

            struct storage_list_node_ current_node;
            if (!storage_list_node__read(fd, &current_node)) {
                storage_list__destroy(storage_list);
                return false;
            }

            storage_list.head = current_node.next;

            storage_list_node__destroy(current_node);
        }

        if (!go_to_offset(fd, list)) {
            storage_list__destroy(storage_list);
            return false;
        }

        if (!storage_list__write(fd, &storage_list)) {
            storage_list__destroy(storage_list);
            return false;
        }

        storage_list__destroy(storage_list);
        return true;
    }

    storage_pointer_ prev_node_ptr = storage_list.head;
    while (prev_node_ptr) {
        if (!go_to_offset(fd, prev_node_ptr)) {
            storage_list__destroy(storage_list);
            return false;
        }

        struct storage_list_node_ prev_node;
        if (!storage_list_node__read(fd, &prev_node)) {
            storage_list__destroy(storage_list);
            return false;
        }

        if (prev_node.next == node) {
            // general case

            if (!go_to_offset(fd, node)) {
                storage_list_node__destroy(prev_node);
                storage_list__destroy(storage_list);
                return false;
            }

            struct storage_list_node_ current_node;
            if (!storage_list_node__read(fd, &current_node)) {
                storage_list_node__destroy(prev_node);
                storage_list__destroy(storage_list);
                return false;
            }

            prev_node.next = current_node.next;

            storage_list_node__destroy(current_node);

            if (!go_to_offset(fd, prev_node_ptr)) {
                storage_list_node__destroy(prev_node);
                storage_list__destroy(storage_list);
                return false;
            }

            if (!storage_list_node__write(fd, &prev_node)) {
                storage_list_node__destroy(prev_node);
                storage_list__destroy(storage_list);
                return false;
            }

            storage_list_node__destroy(prev_node);

            // node is tail of list
            if (storage_list.tail == node) {
                storage_list.tail = prev_node_ptr;

                if (!go_to_offset(fd, list)) {
                    storage_list__destroy(storage_list);
                    return false;
                }

                if (!storage_list__write(fd, &storage_list)) {
                    storage_list__destroy(storage_list);
                    return false;
                }
            }

            storage_list__destroy(storage_list);
            return true;
        }

        prev_node_ptr = prev_node.next;

        storage_list_node__destroy(prev_node);
    }

    storage_list__destroy(storage_list);
    return true;
}

bool storage_init(int fd, storage * storage) {
    if (!storage) {
        errno = EINVAL;
        return false;
    }

    if (!go_to_offset(fd, 0)) {
        return false;
    }

    struct storage_header_ header;

    if (!storage_header__init(&header)) {
        return false;
    }

    if (!storage_header__write(fd, &header)) {
        storage_header__destroy(header);
        return false;
    }

    storage_header__destroy(header);

    storage->fd = fd;
    return storage;
}

bool storage_open(int fd, storage * storage) {
    if (!storage) {
        errno = EINVAL;
        return false;
    }

    if (!go_to_offset(fd, 0)) {
        return false;
    }

    struct storage_header_ header;
    if (!storage_header__read_and_check(fd, &header)) {
        return false;
    }

    storage_header__destroy(header);

    storage->fd = fd;
    return storage;
}

bool storage_get_first_vertex(storage storage, struct storage_vertex * vertex) {
    if (!vertex) {
        return false;
    }

    const int fd = storage.fd;
    if (!go_to_offset(fd, 0)) {
        return false;
    }

    struct storage_header_ header;
    if (!storage_header__read(fd, &header)) {
        return false;
    }

    vertex->pointer.storage = storage;
    vertex->pointer.offset = header.vertices.head;

    storage_header__destroy(header);
    return true;
}

bool storage_get_first_edge(storage storage, struct storage_edge * edge) {
    if (!edge) {
        errno = EINVAL;
        return false;
    }

    const int fd = storage.fd;
    if (!go_to_offset(fd, 0)) {
        return false;
    }

    struct storage_header_ header;
    if (!storage_header__read(fd, &header)) {
        return false;
    }

    edge->pointer.storage = storage;
    edge->pointer.offset = header.edges.head;

    storage_header__destroy(header);
    return true;
}

bool storage_create_vertex(storage storage, struct storage_vertex * vertex) {
    if (!vertex) {
        errno = EINVAL;
        return false;
    }

    const int fd = storage.fd;
    if (!go_to_offset(fd, 0)) {
        return false;
    }

    struct storage_header_ header;
    if (!storage_header__read(fd, &header)) {
        return false;
    }

    storage_pointer_ vertex_ptr;
    if (!go_to_end(fd, &vertex_ptr)) {
        storage_header__destroy(header);
        return false;
    }

    struct storage_vertex_ new_vertex = { 0 };
    if (!storage_vertex__write(fd, &new_vertex)) {
        storage_header__destroy(header);
        return false;
    }

    storage_pointer_ node_ptr;
    if (!add_element_to_list(fd, POINTER_TO_CHILD(0, struct storage_header_, vertices), vertex_ptr, &node_ptr)) {
        storage_header__destroy(header);
        return false;
    }

    vertex->pointer.storage = storage;
    vertex->pointer.offset = node_ptr;

    storage_header__destroy(header);
    return true;
}

bool storage_create_edge(storage storage, struct storage_edge * edge) {
    if (!edge) {
        errno = EINVAL;
        return false;
    }

    const int fd = storage.fd;
    if (!go_to_offset(fd, 0)) {
        return false;
    }

    struct storage_header_ header;
    if (!storage_header__read(fd, &header)) {
        return false;
    }

    storage_pointer_ edge_ptr;
    if (!go_to_end(fd, &edge_ptr)) {
        storage_header__destroy(header);
        return false;
    }

    struct storage_edge_ new_edge = { 0 };
    if (!storage_edge__write(fd, &new_edge)) {
        storage_header__destroy(header);
        return false;
    }

    storage_pointer_ node_ptr;
    if (!add_element_to_list(fd, POINTER_TO_CHILD(0, struct storage_header_, edges), edge_ptr, &node_ptr)) {
        storage_header__destroy(header);
        return false;
    }

    edge->pointer.storage = storage;
    edge->pointer.offset = node_ptr;

    storage_header__destroy(header);
    return true;
}

bool storage_vertex_next(struct storage_vertex * vertex) {
    if (!vertex) {
        errno = EINVAL;
        return false;
    }

    return get_next_list_node_pointer(&(vertex->pointer));
}

bool storage_vertex_has_label(struct storage_vertex vertex, const char * label) {
    if (!label) {
        return false;
    }

    struct storage_label storage_label;
    if (!storage_vertex_get_first_label(vertex, &storage_label)) {
        return false;
    }

    while (storage_label.pointer.offset) {
        char * this_label;
        if (!storage_label_get(storage_label, &this_label)) {
            return false;
        }

        if (strcmp(this_label, label) == 0) {
            free(this_label);
            return true;
        }

        free(this_label);

        if (!storage_label_next(&storage_label)) {
            return false;
        }
    }

    return false;
}

bool storage_vertex_add_label(struct storage_vertex vertex, const char * label) {
    if (!label) {
        errno = EINVAL;
        return false;
    }

    if (storage_vertex_has_label(vertex, label)) {
        return true;
    }

    const int fd = vertex.pointer.storage.fd;
    if (!go_to_offset(fd, vertex.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    storage_pointer_ label_pointer;
    if (!go_to_end(fd, &label_pointer)) {
        storage_list_node__destroy(node);
        return false;
    }

    struct storage_string_ string;
    if (!storage_string__init(label, &string)) {
        storage_list_node__destroy(node);
        return false;
    }

    if (!storage_string__write(fd, &string)) {
        storage_string__destroy(string);
        storage_list_node__destroy(node);
        return false;
    }

    storage_string__destroy(string);

    if (!add_element_to_list(fd, POINTER_TO_CHILD(node.value, struct storage_vertex_, labels), label_pointer, NULL)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);
    return true;
}

bool storage_vertex_remove_label(struct storage_vertex vertex, const char * label) {
    if (!label) {
        return true;
    }

    const int fd = vertex.pointer.storage.fd;

    if (!go_to_offset(fd, vertex.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ vertex_node;
    if (!storage_list_node__read(fd, &vertex_node)) {
        return false;
    }

    if (!go_to_offset(fd, vertex_node.value)) {
        storage_list_node__destroy(vertex_node);
        return false;
    }

    struct storage_vertex_ storage_vertex;
    if (!storage_vertex__read(fd, &storage_vertex)) {
        storage_list_node__destroy(vertex_node);
        return false;
    }

    storage_pointer_ label_node_ptr = storage_vertex.labels.head;
    storage_vertex__destroy(storage_vertex);

    while (label_node_ptr) {
        if (!go_to_offset(fd, label_node_ptr)) {
            storage_list_node__destroy(vertex_node);
            return false;
        }

        struct storage_list_node_ label_node;
        if (!storage_list_node__read(fd, &label_node)) {
            storage_list_node__destroy(vertex_node);
            return false;
        }

        if (!go_to_offset(fd, label_node.value)) {
            storage_list_node__destroy(label_node);
            storage_list_node__destroy(vertex_node);
            return false;
        }

        struct storage_string_ str;
        if (!storage_string__read(fd, &str)) {
            storage_list_node__destroy(label_node);
            storage_list_node__destroy(vertex_node);
            return false;
        }

        if (strcmp(str.value, label) == 0) {
            if (!remove_node_from_list(
                fd, POINTER_TO_CHILD(vertex_node.value, struct storage_vertex_, labels), label_node_ptr
            )) {
                storage_string__destroy(str);
                storage_list_node__destroy(label_node);
                storage_list_node__destroy(vertex_node);
                return false;
            }

            storage_string__destroy(str);
            storage_list_node__destroy(label_node);
            storage_list_node__destroy(vertex_node);
            return true;
        }

        storage_string__destroy(str);

        label_node_ptr = label_node.next;
        storage_list_node__destroy(label_node);
    }

    storage_list_node__destroy(vertex_node);
    return true;
}

bool storage_vertex_get_labels_amount(struct storage_vertex vertex, size_t * amount) {
    if (!amount) {
        errno = EINVAL;
        return false;
    }

    struct storage_label label;
    if (!storage_vertex_get_first_label(vertex, &label)) {
        return false;
    }

    size_t c = 0;
    while (label.pointer.offset) {
        ++c;

        if (!storage_label_next(&label)) {
            return false;
        }
    }

    *amount = c;
    return true;
}

bool storage_vertex_get_first_label(struct storage_vertex vertex, struct storage_label * label) {
    if (!label) {
        errno = EINVAL;
        return false;
    }

    const int fd = vertex.pointer.storage.fd;
    if (!go_to_offset(fd, vertex.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_vertex_ storage_vertex;
    if (!storage_vertex__read(fd, &storage_vertex)) {
        return false;
    }

    label->pointer.storage = vertex.pointer.storage;
    label->pointer.offset = storage_vertex.labels.head;

    storage_vertex__destroy(storage_vertex);
    return true;
}

bool storage_vertex_get_attribute(struct storage_vertex vertex, const char * name, char ** value) {
    if (!name) {
        if (value) {
            *value = NULL;
        }

        return true;
    }

    const int fd = vertex.pointer.storage.fd;
    if (!go_to_offset(fd, vertex.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ vertex_node;
    if (!storage_list_node__read(fd, &vertex_node)) {
        return false;
    }

    if (!go_to_offset(fd, vertex_node.value)) {
        return false;
    }

    storage_list_node__destroy(vertex_node);

    struct storage_vertex_ storage_vertex;
    if (!storage_vertex__read(fd, &storage_vertex)) {
        return false;
    }

    storage_pointer_ attrs_node_ptr = storage_vertex.attributes.head;
    storage_vertex__destroy(storage_vertex);

    while (attrs_node_ptr) {
        if (!go_to_offset(fd, attrs_node_ptr)) {
            return false;
        }

        struct storage_list_node_ attr_node;
        if (!storage_list_node__read(fd, &attr_node)) {
            return false;
        }

        if (!go_to_offset(fd, attr_node.value)) {
            storage_list_node__destroy(attr_node);
            return false;
        }

        struct storage_attribute_ attr;
        if (!storage_attribute__read(fd, &attr)) {
            storage_list_node__destroy(attr_node);
            return false;
        }

        if (strcmp(attr.name.value, name) == 0) {
            if (value) {
                if (!go_to_offset(fd, attr.value)) {
                    storage_attribute__destroy(attr);
                    storage_list_node__destroy(attr_node);
                    return false;
                }

                storage_attribute__destroy(attr);

                struct storage_string_ value_string;
                if (!storage_string__read(fd, &value_string)) {
                    storage_list_node__destroy(attr_node);
                    return false;
                }

                if (value_string.value) {
                    *value = strdup(value_string.value);

                    if (!(*value)) {
                        storage_string__destroy(value_string);
                        storage_list_node__destroy(attr_node);
                        return false;
                    }
                } else {
                    *value = NULL;
                }

                storage_string__destroy(value_string);
            } else {
                storage_attribute__destroy(attr);
            }

            storage_list_node__destroy(attr_node);
            return true;
        }

        storage_attribute__destroy(attr);

        attrs_node_ptr = attr_node.next;
        storage_list_node__destroy(attr_node);
    }

    if (value) {
        *value = NULL;
    }

    return true;
}

bool storage_vertex_set_attribute(struct storage_vertex vertex, const char * name, const char * value) {
    if (!name) {
        errno = EINVAL;
        return false;
    }

    const int fd = vertex.pointer.storage.fd;
    if (!go_to_offset(fd, vertex.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ vertex_node;
    if (!storage_list_node__read(fd, &vertex_node)) {
        return false;
    }

    const storage_pointer_ vertex_ptr = vertex_node.value;

    if (!go_to_offset(fd, vertex_ptr)) {
        storage_list_node__destroy(vertex_node);
        return false;
    }

    storage_list_node__destroy(vertex_node);

    struct storage_vertex_ storage_vertex;
    if (!storage_vertex__read(fd, &storage_vertex)) {
        return false;
    }

    storage_pointer_ attrs_node_ptr = storage_vertex.attributes.head;

    storage_vertex__destroy(storage_vertex);

    while (attrs_node_ptr) {
        if (!go_to_offset(fd, attrs_node_ptr)) {
            return false;
        }

        struct storage_list_node_ attr_node;
        if (!storage_list_node__read(fd, &attr_node)) {
            return false;
        }

        if (!go_to_offset(fd, attr_node.value)) {
            storage_list_node__destroy(attr_node);
            return false;
        }

        struct storage_attribute_ attr;
        if (!storage_attribute__read(fd, &attr)) {
            storage_list_node__destroy(attr_node);
            return false;
        }

        if (strcmp(attr.name.value, name) == 0) {
            if (value) {
                struct storage_string_ value_string;

                if (!storage_string__init(value, &value_string)) {
                    storage_attribute__destroy(attr);
                    storage_list_node__destroy(attr_node);
                    return false;
                }

                storage_pointer_ value_ptr;
                if (!go_to_end(fd, &value_ptr)) {
                    storage_string__destroy(value_string);
                    storage_attribute__destroy(attr);
                    storage_list_node__destroy(attr_node);
                    return false;
                }

                if (!storage_string__write(fd, &value_string)) {
                    storage_string__destroy(value_string);
                    storage_attribute__destroy(attr);
                    storage_list_node__destroy(attr_node);
                    return false;
                }

                attr.value = value_ptr;
                storage_string__destroy(value_string);
            } else {
                attr.value = 0;
            }

            if (!go_to_offset(fd, attr_node.value)) {
                storage_attribute__destroy(attr);
                storage_list_node__destroy(attr_node);
                return false;
            }

            if (!storage_attribute__write(fd, &attr)) {
                storage_attribute__destroy(attr);
                storage_list_node__destroy(attr_node);
                return false;
            }

            storage_attribute__destroy(attr);
            storage_list_node__destroy(attr_node);
            return true;
        }

        storage_attribute__destroy(attr);

        attrs_node_ptr = attr_node.next;

        storage_list_node__destroy(attr_node);
    }

    if (!value) {
        return true;
    }

    struct storage_string_ value_str;
    if (!storage_string__init(value, &value_str)) {
        return false;
    }

    storage_pointer_ value_ptr;
    if (!go_to_end(fd, &value_ptr)) {
        storage_string__destroy(value_str);
        return false;
    }

    if (!storage_string__write(fd, &value_str)) {
        storage_string__destroy(value_str);
        return false;
    }

    storage_string__destroy(value_str);

    struct storage_attribute_ attribute = { 0 };
    if (!storage_string__init(name, &(attribute.name))) {
        return false;
    }

    attribute.value = value_ptr;

    storage_pointer_ attr_ptr;
    if (!go_to_end(fd, &attr_ptr)) {
        storage_attribute__destroy(attribute);
        return false;
    }

    if (!storage_attribute__write(fd, &attribute)) {
        storage_attribute__destroy(attribute);
        return false;
    }

    storage_attribute__destroy(attribute);

    if (!add_element_to_list(fd, POINTER_TO_CHILD(vertex_ptr, struct storage_vertex_, attributes), attr_ptr, NULL)) {
        return false;
    }

    return true;
}

bool storage_vertex_remove_attribute(struct storage_vertex vertex, const char * name) {
    return storage_vertex_set_attribute(vertex, name, NULL);
}

bool storage_vertex_get_attributes_amount(struct storage_vertex vertex, size_t * amount) {
    if (!amount) {
        errno = EINVAL;
        return false;
    }

    struct storage_attribute attribute;
    if (!storage_vertex_get_first_attribute(vertex, &attribute)) {
        return false;
    }

    size_t c = 0;
    while (attribute.pointer.offset) {
        ++c;

        if (!storage_attribute_next(&attribute)) {
            return false;
        }
    }

    *amount = c;
    return true;
}

bool storage_vertex_get_first_attribute(struct storage_vertex vertex, struct storage_attribute * attribute) {
    if (!attribute) {
        errno = EINVAL;
        return false;
    }

    const int fd = vertex.pointer.storage.fd;
    if (!go_to_offset(fd, vertex.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_vertex_ storage_vertex;
    if (!storage_vertex__read(fd, &storage_vertex)) {
        return false;
    }

    attribute->pointer.storage = vertex.pointer.storage;
    attribute->pointer.offset = storage_vertex.attributes.head;

    storage_vertex__destroy(storage_vertex);
    return true;
}

bool storage_vertex_drop(struct storage_vertex vertex) {
    return remove_node_from_list(
        vertex.pointer.storage.fd,
        POINTER_TO_CHILD(0, struct storage_header_, vertices),
        vertex.pointer.offset
    );
}

bool storage_edge_next(struct storage_edge * edge) {
    if (!edge) {
        errno = EINVAL;
        return false;
    }

    return get_next_list_node_pointer(&(edge->pointer));
}

bool storage_edge_get_source(struct storage_edge edge, struct storage_vertex * vertex) {
    if (!vertex) {
        errno = EINVAL;
        return false;
    }

    const int fd = edge.pointer.storage.fd;
    if (!go_to_offset(fd, edge.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_edge_ storage_edge;
    if (!storage_edge__read(fd, &storage_edge)) {
        return false;
    }

    vertex->pointer.storage = edge.pointer.storage;
    vertex->pointer.offset = storage_edge.source;

    storage_edge__destroy(storage_edge);
    return true;
}

bool storage_edge_set_source(struct storage_edge edge, struct storage_vertex vertex) {
    const int fd = edge.pointer.storage.fd;

    if (!go_to_offset(fd, edge.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    struct storage_edge_ storage_edge;
    if (!storage_edge__read(fd, &storage_edge)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_edge.source = vertex.pointer.offset;

    if (!go_to_offset(fd, node.value)) {
        storage_edge__destroy(storage_edge);
        storage_list_node__destroy(node);
        return false;
    }

    if (!storage_edge__write(fd, &storage_edge)) {
        storage_edge__destroy(storage_edge);
        storage_list_node__destroy(node);
        return false;
    }

    storage_edge__destroy(storage_edge);
    storage_list_node__destroy(node);
    return true;
}

bool storage_edge_get_destination(struct storage_edge edge, struct storage_vertex * vertex) {
    if (!vertex) {
        errno = EINVAL;
        return false;
    }

    const int fd = edge.pointer.storage.fd;
    if (!go_to_offset(fd, edge.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_edge_ storage_edge;
    if (!storage_edge__read(fd, &storage_edge)) {
        return false;
    }

    vertex->pointer.storage = edge.pointer.storage;
    vertex->pointer.offset = storage_edge.destination;

    storage_edge__destroy(storage_edge);
    return true;
}

bool storage_edge_set_destination(struct storage_edge edge, struct storage_vertex vertex) {
    const int fd = edge.pointer.storage.fd;

    if (!go_to_offset(fd, edge.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    struct storage_edge_ storage_edge;
    if (!storage_edge__read(fd, &storage_edge)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_edge.destination = vertex.pointer.offset;

    if (!go_to_offset(fd, node.value)) {
        storage_edge__destroy(storage_edge);
        storage_list_node__destroy(node);
        return false;
    }

    if (!storage_edge__write(fd, &storage_edge)) {
        storage_edge__destroy(storage_edge);
        storage_list_node__destroy(node);
        return false;
    }

    storage_edge__destroy(storage_edge);
    storage_list_node__destroy(node);
    return true;
}

bool storage_edge_get_label(struct storage_edge edge, char ** label) {
    if (!label) {
        errno = EINVAL;
        return false;
    }

    const int fd = edge.pointer.storage.fd;
    if (!go_to_offset(fd, edge.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_edge_ storage_edge;
    if (!storage_edge__read(fd, &storage_edge)) {
        return false;
    }

    if (!storage_edge.label) {
        *label = NULL;

        storage_edge__destroy(storage_edge);
        return true;
    }

    if (!go_to_offset(fd, storage_edge.label)) {
        storage_edge__destroy(storage_edge);
        return false;
    }

    storage_edge__destroy(storage_edge);

    struct storage_string_ label_str;
    if (!storage_string__read(fd, &label_str)) {
        return false;
    }

    *label = strdup(label_str.value);
    if (!(*label)) {
        storage_string__destroy(label_str);
        return false;
    }

    storage_string__destroy(label_str);
    return true;
}

bool storage_edge_set_label(struct storage_edge edge, const char * label) {
    const int fd = edge.pointer.storage.fd;

    if (!go_to_offset(fd, edge.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    struct storage_edge_ storage_edge;
    if (!storage_edge__read(fd, &storage_edge)) {
        storage_list_node__destroy(node);
        return false;
    }

    const bool has_label = storage_edge.label != 0;
    struct storage_string_ str;

    if (has_label) {
        if (!go_to_offset(fd, storage_edge.label)) {
            storage_edge__destroy(storage_edge);
            storage_list_node__destroy(node);
            return false;
        }

        if (!storage_string__read(fd, &str)) {
            storage_edge__destroy(storage_edge);
            storage_list_node__destroy(node);
            return false;
        }
    }

    if (label) {
        if (has_label && strcmp(str.value, label) == 0) {
            storage_string__destroy(str);
            storage_edge__destroy(storage_edge);
            storage_list_node__destroy(node);
            return true;
        }

        if (has_label) {
            storage_string__destroy(str);
        }

        if (!storage_string__init(label, &str)) {
            storage_edge__destroy(storage_edge);
            storage_list_node__destroy(node);
            return false;
        }

        storage_pointer_ str_ptr;
        if (!go_to_end(fd, &str_ptr)) {
            storage_string__destroy(str);
            storage_edge__destroy(storage_edge);
            storage_list_node__destroy(node);
            return false;
        }

        if (!storage_string__write(fd, &str)) {
            storage_string__destroy(str);
            storage_edge__destroy(storage_edge);
            storage_list_node__destroy(node);
            return false;
        }

        storage_edge.label = str_ptr;
    } else {
        storage_edge.label = 0;
    }

    storage_string__destroy(str);

    if (!go_to_offset(fd, node.value)) {
        storage_edge__destroy(storage_edge);
        storage_list_node__destroy(node);
        return false;
    }

    if (!storage_edge__write(fd, &storage_edge)) {
        storage_edge__destroy(storage_edge);
        storage_list_node__destroy(node);
        return false;
    }

    storage_edge__destroy(storage_edge);
    storage_list_node__destroy(node);
    return true;
}

bool storage_edge_remove_label(struct storage_edge edge, const char * label) {
    if (!label) {
        return false;
    }

    char * current_label;
    if (!storage_edge_get_label(edge, &current_label)) {
        return false;
    }

    if (strcmp(current_label, label) != 0) {
        free(current_label);
        return true;
    }

    free(current_label);

    return storage_edge_set_label(edge, NULL);
}

bool storage_edge_drop(struct storage_edge edge) {
    return remove_node_from_list(
        edge.pointer.storage.fd,
        POINTER_TO_CHILD(0, struct storage_header_, edges),
        edge.pointer.offset
    );
}

bool storage_label_next(struct storage_label * label) {
    if (!label) {
        errno = EINVAL;
        return false;
    }

    return get_next_list_node_pointer(&(label->pointer));
}

bool storage_label_get(struct storage_label label, char ** value) {
    if (!value) {
        errno = EINVAL;
        return false;
    }

    const int fd = label.pointer.storage.fd;
    if (!go_to_offset(fd, label.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_string_ str;
    if (!storage_string__read(fd, &str)) {
        return false;
    }

    *value = strdup(str.value);
    if (!(*value)) {
        storage_string__destroy(str);
        return false;
    }

    storage_string__destroy(str);
    return true;
}

bool storage_attribute_next(struct storage_attribute * attribute) {
    if (!attribute) {
        errno = EINVAL;
        return false;
    }

    return get_next_list_node_pointer(&(attribute->pointer));
}

bool storage_attribute_get(struct storage_attribute attribute, char ** name, char ** value) {
    if (!name && !value) {
        errno = EINVAL;
        return false;
    }

    const int fd = attribute.pointer.storage.fd;
    if (!go_to_offset(fd, attribute.pointer.offset)) {
        return false;
    }

    struct storage_list_node_ node;
    if (!storage_list_node__read(fd, &node)) {
        return false;
    }

    if (!go_to_offset(fd, node.value)) {
        storage_list_node__destroy(node);
        return false;
    }

    storage_list_node__destroy(node);

    struct storage_attribute_ attr;
    if (!storage_attribute__read(fd, &attr)) {
        return false;
    }

    if (name) {
        *name = strdup(attr.name.value);

        if (!(*name)) {
            storage_attribute__destroy(attr);
            return false;
        }
    }

    if (value) {
        if (!go_to_offset(fd, attr.value)) {
            storage_attribute__destroy(attr);
            return false;
        }

        struct storage_string_ str;
        if (!storage_string__read(fd, &str)) {
            storage_attribute__destroy(attr);
            return false;
        }

        *value = strdup(str.value);
        if (!(*value)) {
            storage_string__destroy(str);
            storage_attribute__destroy(attr);
            return false;
        }

        storage_string__destroy(str);
    }

    storage_attribute__destroy(attr);
    return true;
}
