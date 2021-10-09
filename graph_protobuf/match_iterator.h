#pragma once

#include <stddef.h>

#include "storage.h"


struct match_iterator_definition {
    struct {
        size_t amount;

        struct {
            const char * name;
            struct {
                size_t amount;
                const char ** labels;
            } labels;

            struct {
                size_t amount;

                struct {
                    const char * name;
                    const char * value;
                } * attributes;
            } attributes;
        } * vertices;
    } vertices;

    struct {
        size_t amount;

        struct {
            const char * name;
            size_t source_idx;
            size_t destination_idx;
            const char * label;
        } * edges;
    } edges;
};

struct match_iterator {
    const struct match_iterator_definition * definition;

    bool started;
    struct storage_vertex * vertices;
    struct storage_edge * edges;
};


void match_iterator_definition_destroy(struct match_iterator_definition definition);

bool match_iterator_init(storage storage, struct match_iterator * iterator, const struct match_iterator_definition * definition);
void match_iterator_destroy(struct match_iterator iterator);

// make iterator points to element;
// must be called before first usage of iterator.
//
// returns true if iterator points to valid element;
// returns false if there aren't elements to point to;
// returns false (with errno != 0) if an error occurred.
bool match_iterator_next(struct match_iterator * iterator);
