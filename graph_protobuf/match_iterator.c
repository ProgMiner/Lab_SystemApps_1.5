#include "match_iterator.h"

#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "storage.h"
#include "utils.h"


void match_iterator_definition_destroy(struct match_iterator_definition definition) {
    for (size_t i = 0; i < definition.vertices.amount; ++i) {
        free(definition.vertices.vertices[i].labels.labels);
        free(definition.vertices.vertices[i].attributes.attributes);
    }
}

bool match_iterator_init(storage storage, struct match_iterator * iterator, const struct match_iterator_definition * definition) {
    iterator->definition = definition;
    iterator->started = false;

    iterator->vertices = calloc(definition->vertices.amount, sizeof(struct storage_vertex));
    if (!iterator->vertices) {
        return false;
    }

    iterator->edges = calloc(definition->edges.amount, sizeof(struct storage_edge));
    if (!iterator->edges) {
        free(iterator->vertices);
        return false;
    }

    for (size_t i = 0; i < definition->vertices.amount; ++i) {
        if (!storage_get_first_vertex(storage, &(iterator->vertices[i]))) {
            goto free_all;
        }
    }

    for (size_t i = 0; i < definition->edges.amount; ++i) {
        if (!storage_get_first_edge(storage, &(iterator->edges[i]))) {
            goto free_all;
        }
    }

    return true;

free_all:
    free(iterator->vertices);
    free(iterator->edges);

    return false;
}

void match_iterator_destroy(struct match_iterator iterator) {
    free(iterator.vertices);
    free(iterator.edges);
}

static bool match_iterator_next_vertex(struct match_iterator iterator, size_t idx, bool * carry) {
    if (carry) {
        *carry = false;
    }

    if (idx > iterator.definition->vertices.amount) {
        return true;
    }

    if (iterator.vertices[idx].pointer.offset == 0) {
        errno = 0;
        return false;
    }

    if (!storage_vertex_next(&(iterator.vertices[idx]))) {
        return false;
    }

    if (iterator.vertices[idx].pointer.offset) {
        return true;
    }

    // if there aren't more vertices - repeat
    if (!storage_get_first_vertex(iterator.vertices[idx].pointer.storage, &(iterator.vertices[idx]))) {
        return false;
    }

    if (carry) {
        *carry = true;
    }

    return true;
}

static bool match_iterator_next_edge(struct match_iterator iterator, size_t idx, bool * carry) {
    if (carry) {
        *carry = false;
    }

    if (idx > iterator.definition->edges.amount) {
        return true;
    }

    if (iterator.edges[idx].pointer.offset == 0) {
        errno = 0;
        return false;
    }

    if (!storage_edge_next(&(iterator.edges[idx]))) {
        return false;
    }

    if (iterator.edges[idx].pointer.offset) {
        return true;
    }

    // if there aren't more edges - repeat
    if (!storage_get_first_edge(iterator.edges[idx].pointer.storage, &(iterator.edges[idx]))) {
        return false;
    }

    if (carry) {
        *carry = true;
    }

    return true;
}

static inline size_t match_iterator_entities_amount(struct match_iterator iterator) {
    return iterator.definition->vertices.amount + iterator.definition->edges.amount;
}

static bool match_iterator_next_entity(struct match_iterator iterator, size_t idx, bool * carry) {
    if (idx < iterator.definition->vertices.amount) {
        return match_iterator_next_vertex(iterator, idx, carry);
    }

    return match_iterator_next_edge(iterator, idx - iterator.definition->vertices.amount, carry);
}

static bool match_iterator_next_0(struct match_iterator iterator) {
    const size_t entities_amount = match_iterator_entities_amount(iterator);

    bool carry = true;
    for (size_t i = entities_amount; i > 0; --i) {
        if (!carry) {
            break;
        }

        if (!match_iterator_next_entity(iterator, i - 1, &carry)) {
            return false;
        }
    }

    if (carry) {
        errno = 0;
        return false;
    }

    return true;
}

static bool match_iterator_vertex_applicable(struct match_iterator iterator, size_t idx) {
    for (size_t j = 0; j < iterator.definition->vertices.vertices[idx].labels.amount; ++j) {
        const char * const label = iterator.definition->vertices.vertices[idx].labels.labels[j];

        if (!storage_vertex_has_label(iterator.vertices[idx], label)) {
            return false;
        }
    }

    for (size_t j = 0; j < iterator.definition->vertices.vertices[idx].attributes.amount; ++j) {
        const char * const name = iterator.definition->vertices.vertices[idx].attributes.attributes[j].name;
        const char * const value = iterator.definition->vertices.vertices[idx].attributes.attributes[j].value;

        char * actual_value;
        if (!storage_vertex_get_attribute(iterator.vertices[idx], name, &actual_value)) {
            return false;
        }

        if (actual_value == NULL || strcmp(actual_value, value) != 0) {
            free(actual_value);
            return false;
        }

        free(actual_value);
    }

    return true;
}

static bool match_iterator_edge_applicable(struct match_iterator iterator, size_t idx) {
    struct storage_vertex const definition_source = iterator.vertices[iterator.definition->edges.edges[idx].source_idx];
    struct storage_vertex const definition_destination = iterator.vertices[iterator.definition->edges.edges[idx].destination_idx];

    struct storage_vertex actual;
    if (!storage_edge_get_source(iterator.edges[idx], &actual)) {
        return false;
    }

    if (!storage_vertex_equals(actual, definition_source)) {
        return false;
    }

    if (!storage_edge_get_destination(iterator.edges[idx], &actual)) {
        return false;
    }

    if (!storage_vertex_equals(actual, definition_destination)) {
        return false;
    }

    if (iterator.definition->edges.edges[idx].label) {
        char * label;

        if (!storage_edge_get_label(iterator.edges[idx], &label)) {
            return false;
        }

        if (label && strcmp(label, iterator.definition->edges.edges[idx].label) != 0) {
            free(label);
            return false;
        }

        free(label);
    }

    return true;
}

static bool match_iterator_applicable(struct match_iterator iterator) {
    for (size_t i = 0; i < iterator.definition->vertices.amount; ++i) {
        if (!match_iterator_vertex_applicable(iterator, i)) {
            return false;
        }
    }

    for (size_t i = 0; i < iterator.definition->edges.amount; ++i) {
        if (!match_iterator_edge_applicable(iterator, i)) {
            return false;
        }
    }

    return true;
}

static bool match_iterator_roll(struct match_iterator iterator) {
    while (!match_iterator_applicable(iterator)) {
        if (!match_iterator_next_0(iterator)) {
            return false;
        }
    }

    if (errno) {
        return false;
    }

    return true;
}

bool match_iterator_next(struct match_iterator * iterator) {
    if (iterator->started) {
        if (!match_iterator_next_0(*iterator)) {
            return false;
        }
    } else {
        iterator->started = true;

        for (size_t i = 0; i < iterator->definition->vertices.amount; ++i) {
            if (!iterator->vertices[i].pointer.offset) {
                errno = 0;
                return false;
            }
        }

        for (size_t i = 0; i < iterator->definition->edges.amount; ++i) {
            if (!iterator->edges[i].pointer.offset) {
                errno = 0;
                return false;
            }
        }
    }

    return match_iterator_roll(*iterator);
}
