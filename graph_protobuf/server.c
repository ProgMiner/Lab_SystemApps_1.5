#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <inttypes.h>
#include <signal.h>

#include "storage.h"
#include "match_iterator.h"
#include "api.pb-c.h"
#include "utils.h"


#define LIMIT_MAX 1000
#define LIMIT_DEFAULT 10


struct request_context {
    storage storage;

    const Where * where;
    const struct match_iterator_definition * match_definition;

    Response * response;
};


static volatile bool closing = false;

static void close_handler(int sig, siginfo_t * info, void * context) {
    closing = true;
}

static bool create_match_definition(
    size_t n_entities,
    Entity ** entities,
    struct match_iterator_definition * definition,
    Response * response
) {
    definition->vertices.amount = 0;
    definition->vertices.vertices = NULL;

    definition->edges.amount = 0;
    definition->edges.edges = NULL;

    if (n_entities == 0) {
        return true;
    }

    size_t vertices_capacity = 2;
    size_t edges_capacity = 2;

    definition->vertices.vertices = malloc(sizeof(*definition->vertices.vertices) * vertices_capacity);
    if (!definition->vertices.vertices) {
        return false;
    }

    definition->edges.edges = malloc(sizeof(*definition->edges.edges) * edges_capacity);
    if (!definition->edges.edges) {
        free(definition->vertices.vertices);
        return false;
    }

    for (size_t i = 0; i < n_entities; ++i) {
        switch (entities[i]->entity_case) {
            case ENTITY__ENTITY_VERTEX: {
                const VertexEntity * vertex = entities[i]->vertex;

                if (vertex->name) {
                    for (size_t j = 0; j < definition->vertices.amount; ++j) {
                        const char * v = definition->vertices.vertices[j].name;

                        if (v && strcmp(v, vertex->name) == 0) {
                            response->payload_case = RESPONSE__PAYLOAD_ERROR;
                            response->error = "you cannot redefine vertex names";
                            goto response_error;
                        }
                    }

                    for (size_t j = 0; j < definition->edges.amount; ++j) {
                        const char * v = definition->edges.edges[j].name;

                        if (v && strcmp(v, vertex->name) == 0) {
                            response->payload_case = RESPONSE__PAYLOAD_ERROR;
                            response->error = "vertex name overlaps edge name";
                            goto response_error;
                        }
                    }
                }

                if (definition->vertices.amount == vertices_capacity) {
                    const size_t new_vertices_capacity = vertices_capacity * 2;
                    void * const tmp = realloc(definition->vertices.vertices, sizeof(*definition->vertices.vertices) * new_vertices_capacity);

                    if (!tmp) {
                        goto free_all;
                    }

                    vertices_capacity = new_vertices_capacity;
                    definition->vertices.vertices = tmp;
                }

                const size_t idx = definition->vertices.amount;
                ++definition->vertices.amount;

                definition->vertices.vertices[idx].name = vertex->name;

                definition->vertices.vertices[idx].labels.amount = vertex->n_labels;
                definition->vertices.vertices[idx].labels.labels = malloc(sizeof(char *) * vertex->n_labels);
                if (!definition->vertices.vertices[idx].labels.labels) {
                    definition->vertices.vertices[idx].attributes.attributes = NULL;
                    goto free_all;
                }

                definition->vertices.vertices[idx].attributes.amount = vertex->n_attrs;
                definition->vertices.vertices[idx].attributes.attributes = malloc(sizeof(* definition->vertices.vertices[idx].attributes.attributes) * vertex->n_attrs);
                if (!definition->vertices.vertices[idx].attributes.attributes) {
                    goto free_all;
                }

                for (size_t j = 0; j < vertex->n_labels; ++j) {
                    definition->vertices.vertices[idx].labels.labels[j] = vertex->labels[j];
                }

                for (size_t j = 0; j < vertex->n_attrs; ++j) {
                    definition->vertices.vertices[idx].attributes.attributes[j].name = vertex->attrs[j]->name;
                    definition->vertices.vertices[idx].attributes.attributes[j].value = vertex->attrs[j]->value;
                }

                break;
            }

            case ENTITY__ENTITY_EDGE: {
                const EdgeEntity * edge = entities[i]->edge;

                if (edge->name) {
                    for (size_t j = 0; j < definition->vertices.amount; ++j) {
                        const char * v = definition->vertices.vertices[j].name;

                        if (v && strcmp(v, edge->name) == 0) {
                            response->payload_case = RESPONSE__PAYLOAD_ERROR;
                            response->error = "edge name overlaps vertex name";
                            goto response_error;
                        }
                    }

                    for (size_t j = 0; j < definition->edges.amount; ++j) {
                        const char * v = definition->edges.edges[j].name;

                        if (v && strcmp(v, edge->name) == 0) {
                            response->payload_case = RESPONSE__PAYLOAD_ERROR;
                            response->error = "you cannot redefine edge names";
                            goto response_error;
                        }
                    }
                }

                {
                    const VertexEntity * const edge_vertices[2] = { edge->source, edge->destination };
                    for (size_t j = 0; j < 2; ++j) {
                        const VertexEntity * vertex = edge_vertices[j];

                        if (vertex->name) {
                            if (vertex->n_labels > 0 && vertex->n_attrs > 0) {
                                for (size_t k = 0; k < definition->vertices.amount; ++k) {
                                    const char * v = definition->vertices.vertices[k].name;

                                    if (v && strcmp(v, vertex->name) == 0) {
                                        response->payload_case = RESPONSE__PAYLOAD_ERROR;
                                        response->error = "you cannot redefine vertex names";
                                        goto response_error;
                                    }
                                }
                            }

                            for (size_t k = 0; k < definition->edges.amount; ++k) {
                                const char * v = definition->edges.edges[k].name;

                                if (v && strcmp(v, vertex->name) == 0) {
                                    response->payload_case = RESPONSE__PAYLOAD_ERROR;
                                    response->error = "vertex name overlaps edge name";
                                    goto response_error;
                                }
                            }
                        } else {
                            response->payload_case = RESPONSE__PAYLOAD_ERROR;
                            response->error = "vertices of edges must have defined names";
                            goto response_error;
                        }
                    }
                }

                size_t source_idx, destination_idx;

                {
                    bool found = false;

                    for (size_t j = 0; j < definition->vertices.amount; ++j) {
                        const char * v = definition->vertices.vertices[j].name;

                        if (v && strcmp(v, edge->source->name) == 0) {
                            source_idx = j;
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        response->payload_case = RESPONSE__PAYLOAD_ERROR;
                        response->error = "vertices of edges must have defined names";
                        goto response_error;
                    }
                }

                {
                    bool found = false;

                    for (size_t j = 0; j < definition->vertices.amount; ++j) {
                        const char * v = definition->vertices.vertices[j].name;

                        if (v && strcmp(v, edge->destination->name) == 0) {
                            destination_idx = j;
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        response->payload_case = RESPONSE__PAYLOAD_ERROR;
                        response->error = "vertices of edges must have defined names";
                        goto response_error;
                    }
                }

                if (definition->edges.amount == edges_capacity) {
                    const size_t new_edges_capacity = edges_capacity * 2;
                    void * const tmp = realloc(definition->edges.edges, sizeof(*definition->edges.edges) * new_edges_capacity);

                    if (!tmp) {
                        goto free_all;
                    }

                    edges_capacity = new_edges_capacity;
                    definition->edges.edges = tmp;
                }

                const size_t idx = definition->edges.amount;
                ++definition->edges.amount;

                definition->edges.edges[idx].name = edge->name;
                definition->edges.edges[idx].source_idx = source_idx;
                definition->edges.edges[idx].destination_idx = destination_idx;
                definition->edges.edges[idx].label = edge->label;

                break;
            }

            default:
                response->payload_case = RESPONSE__PAYLOAD_ERROR;
                response->error = "bad request";
                goto response_error;
        }
    }

    return true;

response_error:
    errno = 0;

free_all:
    for (size_t i = 0; i < definition->vertices.amount; ++i) {
        free(definition->vertices.vertices[i].labels.labels);
        free(definition->vertices.vertices[i].attributes.attributes);
    }

    free(definition->vertices.vertices);
    free(definition->edges.edges);
    return false;
}

static bool check_where_name(const struct match_iterator_definition * definition, Where where) {
    const char * name;

    switch (where.op_case) {
        case WHERE__OP_EQUALS:
            name = where.equals->name;
            break;

        case WHERE__OP_LABEL:
            name = where.label->name;
            break;

        default:
            errno = EINVAL;
            return false;
    }

    for (size_t i = 0; i < definition->vertices.amount; ++i) {
        const char * v = definition->vertices.vertices[i].name;

        if (v && strcmp(v, name) == 0) {
            return true;
        }
    }

    if (where.op_case != WHERE__OP_EQUALS) {
        for (size_t i = 0; i < definition->edges.amount; ++i) {
            const char * v = definition->edges.edges[i].name;

            if (v && strcmp(v, name) == 0) {
                return true;
            }
        }
    }

    return false;
}

static bool check_where(const struct match_iterator_definition * definition, const Where * where, const char ** error) {
    if (!where) {
        return true;
    }

    if (!definition) {
        *error = "you cannot specify where without match";
        return false;
    }

    switch (where->op_case) {
        case WHERE__OP_EQUALS:
        case WHERE__OP_LABEL:
            if (!check_where_name(definition, *where)) {
                if (where->op_case == WHERE__OP_EQUALS) {
                    *error = "you can check attributes only of defined vertex names";
                } else {
                    *error = "you cannot use in where undefined names";
                }

                return false;
            }

            return true;

        case WHERE__OP_NOT:
            return where->not_ && check_where(definition, where->not_, error);

        case WHERE__OP_AND:
            return where->and_->left && check_where(definition, where->and_->left, error)
                && where->and_->right && check_where(definition, where->and_->right, error);

        case WHERE__OP_OR:
            return where->or_->left && check_where(definition, where->or_->left, error)
                && where->or_->right && check_where(definition, where->or_->right, error);

        default:
            *error = "bad request";
            return false;
    }
}

static bool check_match_iterator_where(struct match_iterator iterator, const Where * where) {
    if (!where) {
        return true;
    }

    switch (where->op_case) {
        case WHERE__OP_EQUALS: {
            size_t idx = 0;

            for (size_t i = 0; i < iterator.definition->vertices.amount; ++i) {
                const char * v = iterator.definition->vertices.vertices[i].name;

                if (v && strcmp(v, where->equals->name) == 0) {
                    idx = i;
                    break;
                }
            }

            char * actual_value;
            if (!storage_vertex_get_attribute(iterator.vertices[idx], where->equals->attr, &actual_value)) {
                return false;
            }

            if (actual_value == NULL) {
                errno = 0;
                return false;
            }

            const bool result = strcmp(actual_value, where->equals->value) == 0;

            free(actual_value);

            errno = 0;
            return result;
        }

        case WHERE__OP_LABEL: {
            bool vertex = false;
            size_t idx = 0;

            for (size_t i = 0; i < iterator.definition->vertices.amount; ++i) {
                const char * v = iterator.definition->vertices.vertices[i].name;

                if (v && strcmp(v, where->label->name) == 0) {
                    idx = i;
                    vertex = true;
                    break;
                }
            }

            if (vertex) {
                return storage_vertex_has_label(iterator.vertices[idx], where->label->label);
            }

            for (size_t i = 0; i < iterator.definition->edges.amount; ++i) {
                const char * v = iterator.definition->edges.edges[i].name;

                if (v && strcmp(v, where->label->name) == 0) {
                    idx = i;
                    break;
                }
            }

            char * label;
            if (!storage_edge_get_label(iterator.edges[idx], &label)) {
                return false;
            }

            errno = 0;
            return label != NULL && strcmp(label, where->label->label) == 0;
        }

        case WHERE__OP_NOT: {
            const bool result = check_match_iterator_where(iterator, where->not_);

            if (!result && errno) {
                return false;
            }

            return !result;
        }

        case WHERE__OP_AND:
            return check_match_iterator_where(iterator, where->and_->left)
                && check_match_iterator_where(iterator, where->and_->right);

        case WHERE__OP_OR: {
            const bool left = check_match_iterator_where(iterator, where->or_->left);

            if (left || (!left && errno)) {
                return left;
            }

            return check_match_iterator_where(iterator, where->or_->right);
        }

        default:
            errno = EINVAL;
            return false;
    }
}

static bool match_iterator_next_where(struct match_iterator * iterator, const Where * where) {
    errno = 0;

    do {
        if (errno) {
            return false;
        }

        if (!match_iterator_next(iterator)) {
            return false;
        }
    } while (!check_match_iterator_where(*iterator, where));

    return !errno;
}

static bool handle_create_request(const RequestCreateOp * request, struct request_context context) {
    if (!context.match_definition) {
        struct match_iterator_definition definition;
        bool result = true;

        if (!create_match_definition(request->n_entities, request->entities, &definition, context.response)) {
            return !errno;
        }

        struct storage_vertex * const vertices = malloc(sizeof(struct storage_vertex) * definition.vertices.amount);
        if (!vertices) {
            result = false;
            goto free_definition;
        }

        uint64_t created = 0;
        for (size_t i = 0; i < definition.vertices.amount; ++i) {
            if (!storage_create_vertex(context.storage, &(vertices[i]))) {
                result = false;
                goto free_vertices;
            }

            for (size_t j = 0; j < definition.vertices.vertices[i].labels.amount; ++j) {
                if (!storage_vertex_add_label(vertices[i], definition.vertices.vertices[i].labels.labels[j])) {
                    result = false;
                    goto free_vertices;
                }
            }

            for (size_t j = 0; j < definition.vertices.vertices[i].attributes.amount; ++j) {
                if (!storage_vertex_set_attribute(vertices[i],
                    definition.vertices.vertices[i].attributes.attributes[j].name,
                    definition.vertices.vertices[i].attributes.attributes[j].value)) {
                    result = false;
                    goto free_vertices;
                }
            }

            ++created;
        }

        for (size_t i = 0; i < definition.edges.amount; ++i) {
            struct storage_edge edge;

            if (!storage_create_edge(context.storage, &edge)) {
                result = false;
                goto free_vertices;
            }

            if (!storage_edge_set_source(edge, vertices[definition.edges.edges[i].source_idx])) {
                result = false;
                goto free_vertices;
            }

            if (!storage_edge_set_destination(edge, vertices[definition.edges.edges[i].destination_idx])) {
                result = false;
                goto free_vertices;
            }

            if (!storage_edge_set_label(edge, definition.edges.edges[i].label)) {
                result = false;
                goto free_vertices;
            }

            ++created;
        }

        context.response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
        context.response->success = malloc(sizeof(ResponseSuccess));
        *(context.response->success) = (ResponseSuccess) RESPONSE_SUCCESS__INIT;
        context.response->success->value_case = RESPONSE_SUCCESS__VALUE_AMOUNT;
        context.response->success->amount = created;

free_vertices:
        free(vertices);

free_definition:
        match_iterator_definition_destroy(definition);
        return result;
    }

    for (size_t i = 0; i < request->n_entities; ++i) {
        if (request->entities[i]->entity_case == ENTITY__ENTITY_VERTEX) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you cannot create vertices with match";
            return true;
        }

        if (request->entities[i]->entity_case != ENTITY__ENTITY_EDGE) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "bad request";
            return true;
        }

        bool source_found = false, destination_found = false;
        for (size_t j = 0; j < context.match_definition->vertices.amount; ++j) {
            const char * name = context.match_definition->vertices.vertices[j].name;

            if (name == NULL) {
                continue;
            }

            if (strcmp(name, request->entities[i]->edge->source->name) == 0) {
                source_found = true;
            }

            if (strcmp(name, request->entities[i]->edge->destination->name) == 0) {
                destination_found = true;
            }

            if (source_found && destination_found) {
                break;
            }
        }

        if (!source_found || !destination_found) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you must use as source and destination of edges only vertex names";
            return true;
        }
    }

    size_t * const source_indexes = malloc(sizeof(size_t) * request->n_entities);
    if (!source_indexes) {
        return false;
    }

    size_t * const destination_indexes = malloc(sizeof(size_t) * request->n_entities);
    if (!destination_indexes) {
        free(source_indexes);
        return false;
    }

    bool result = true;

    struct match_iterator iterator;
    if (!match_iterator_init(context.storage, &iterator, context.match_definition)) {
        result = false;
        goto free_indexes;
    }

    for (size_t i = 0; i < request->n_entities; ++i) {
        for (size_t j = 0; j < context.match_definition->vertices.amount; ++j) {
            const char * name = context.match_definition->vertices.vertices[j].name;

            if (name == NULL) {
                continue;
            }

            if (strcmp(name, request->entities[i]->edge->source->name) == 0) {
                source_indexes[i] = j;
            }

            if (strcmp(name, request->entities[i]->edge->destination->name) == 0) {
                destination_indexes[i] = j;
            }
        }
    }

    uint64_t created = 0;
    while (match_iterator_next_where(&iterator, context.where)) {
        for (size_t i = 0; i < request->n_entities; ++i) {
            struct storage_edge edge;

            if (!storage_create_edge(context.storage, &edge)) {
                break;
            }

            if (!storage_edge_set_source(edge, iterator.vertices[source_indexes[i]])) {
                break;
            }

            if (!storage_edge_set_destination(edge, iterator.vertices[destination_indexes[i]])) {
                break;
            }

            if (!storage_edge_set_label(edge, request->entities[i]->edge->label)) {
                break;
            }

            ++created;
        }

        if (errno) {
            break;
        }
    }

    if (!errno) {
        context.response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
        context.response->success = malloc(sizeof(ResponseSuccess));
        *(context.response->success) = (ResponseSuccess) RESPONSE_SUCCESS__INIT;
        context.response->success->value_case = RESPONSE_SUCCESS__VALUE_AMOUNT;
        context.response->success->amount = created;
    }

    result = !errno;

    match_iterator_destroy(iterator);

free_indexes:
    free(source_indexes);
    free(destination_indexes);

    return result;
}

static bool handle_set_request(const RequestSetOp * request, struct request_context context) {
    if (!context.match_definition) {
        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
        context.response->error = "you cannot set without match";
        return true;
    }

    for (size_t i = 0; i < request->n_changes; ++i) {
        const RequestSetOpChange * const change = request->changes[i];

        switch (change->payload_case) {
            case REQUEST_SET_OP_CHANGE__PAYLOAD_ATTR:
            case REQUEST_SET_OP_CHANGE__PAYLOAD_LABEL:
                break;

            default:
                context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
                context.response->error = "bad request";
                return true;
        }
    }

    bool * const is_edge = malloc(sizeof(bool) * request->n_changes);
    if (!is_edge) {
        return false;
    }

    bool result = true;

    size_t * const indexes = malloc(sizeof(size_t) * request->n_changes);
    if (!indexes) {
        result = false;
        goto free_is_edge;
    }

    for (size_t i = 0; i < request->n_changes; ++i) {
        const RequestSetOpChange * const change = request->changes[i];

        bool found = false;
        for (size_t j = 0; j < context.match_definition->vertices.amount; ++j) {
            const char * name = context.match_definition->vertices.vertices[j].name;

            if (name && strcmp(name, change->name) == 0) {
                is_edge[i] = false;
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (found) {
            continue;
        }

        if (change->payload_case == REQUEST_SET_OP_CHANGE__PAYLOAD_ATTR) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you can set attributes only for vertices";
            goto free_indexes;
        }

        for (size_t j = 0; j < context.match_definition->edges.amount; ++j) {
            const char * name = context.match_definition->edges.edges[j].name;

            if (name && strcmp(name, change->name) == 0) {
                is_edge[i] = true;
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (!found) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you cannot set anything of undefined names";
            goto free_indexes;
        }
    }

    struct match_iterator iterator;
    if (!match_iterator_init(context.storage, &iterator, context.match_definition)) {
        result = false;
        goto free_indexes;
    }

    uint64_t changed = 0;
    while (match_iterator_next_where(&iterator, context.where)) {
        for (size_t i = 0; i < request->n_changes; ++i) {
            const RequestSetOpChange * change = request->changes[i];

            switch (change->payload_case) {
                case REQUEST_SET_OP_CHANGE__PAYLOAD_ATTR: {
                    if (is_edge[i]) {
                        // unreachable
                        continue;
                    }

                    const RequestSetOpChangeAttr * attr = change->attr;
                    if (!storage_vertex_set_attribute(iterator.vertices[indexes[i]], attr->attr, attr->value)) {
                        result = false;
                        goto break_while;
                    }

                    break;
                }

                case REQUEST_SET_OP_CHANGE__PAYLOAD_LABEL: {
                    const char * label = change->label;

                    if (is_edge[i]) {
                        if (!storage_edge_set_label(iterator.edges[indexes[i]], label)) {
                            result = false;
                            goto break_while;
                        }
                    } else {
                        if (!storage_vertex_add_label(iterator.vertices[indexes[i]], label)) {
                            result = false;
                            goto break_while;
                        }
                    }

                    break;
                }

                default:
                    ; // unreachable
            }

            ++changed;
        }
    }

break_while:
    if (!errno) {
        context.response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
        context.response->success = malloc(sizeof(ResponseSuccess));
        *(context.response->success) = (ResponseSuccess) RESPONSE_SUCCESS__INIT;
        context.response->success->value_case = RESPONSE_SUCCESS__VALUE_AMOUNT;
        context.response->success->amount = changed;
    }

    result = !errno;

    match_iterator_destroy(iterator);

free_indexes:
    free(indexes);

free_is_edge:
    free(is_edge);

    return result;
}

static bool handle_remove_request(const RequestRemoveOp * request, struct request_context context) {
    if (!context.match_definition) {
        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
        context.response->error = "you cannot remove without match";
        return true;
    }

    for (size_t i = 0; i < request->n_changes; ++i) {
        const RequestRemoveOpChange * const change = request->changes[i];

        switch (change->payload_case) {
            case REQUEST_REMOVE_OP_CHANGE__PAYLOAD_ATTR:
            case REQUEST_REMOVE_OP_CHANGE__PAYLOAD_LABEL:
                break;

            default:
                context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
                context.response->error = "bad request";
                return true;
        }
    }

    bool * const is_edge = malloc(sizeof(bool) * request->n_changes);
    if (!is_edge) {
        return false;
    }

    bool result = true;

    size_t * const indexes = malloc(sizeof(size_t) * request->n_changes);
    if (!indexes) {
        result = false;
        goto free_is_edge;
    }

    for (size_t i = 0; i < request->n_changes; ++i) {
        const RequestRemoveOpChange * const change = request->changes[i];

        bool found = false;
        for (size_t j = 0; j < context.match_definition->vertices.amount; ++j) {
            const char * name = context.match_definition->vertices.vertices[j].name;

            if (name && strcmp(name, change->name) == 0) {
                is_edge[i] = false;
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (found) {
            continue;
        }

        if (change->payload_case == REQUEST_REMOVE_OP_CHANGE__PAYLOAD_ATTR) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you can remove attributes only for vertices";
            goto free_indexes;
        }

        for (size_t j = 0; j < context.match_definition->edges.amount; ++j) {
            const char * name = context.match_definition->edges.edges[j].name;

            if (name && strcmp(name, change->name) == 0) {
                is_edge[i] = true;
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (!found) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you cannot set anything of undefined names";
            goto free_indexes;
        }
    }

    struct match_iterator iterator;
    if (!match_iterator_init(context.storage, &iterator, context.match_definition)) {
        result = false;
        goto free_indexes;
    }

    uint64_t changed = 0;
    while (match_iterator_next_where(&iterator, context.where)) {
        for (size_t i = 0; i < request->n_changes; ++i) {
            const RequestRemoveOpChange * change = request->changes[i];

            switch (change->payload_case) {
                case REQUEST_REMOVE_OP_CHANGE__PAYLOAD_ATTR: {
                    if (is_edge[i]) {
                        // unreachable
                        continue;
                    }

                    const char * attr = change->attr;
                    if (!storage_vertex_remove_attribute(iterator.vertices[indexes[i]], attr)) {
                        result = false;
                        goto break_while;
                    }

                    break;
                }

                case REQUEST_REMOVE_OP_CHANGE__PAYLOAD_LABEL: {
                    const char * label = change->label;

                    if (is_edge[i]) {
                        if (!storage_edge_remove_label(iterator.edges[indexes[i]], label)) {
                            result = false;
                            goto break_while;
                        }
                    } else {
                        if (!storage_vertex_remove_label(iterator.vertices[indexes[i]], label)) {
                            result = false;
                            goto break_while;
                        }
                    }

                    break;
                }

                default:
                    ; // unreachable
            }

            ++changed;
        }
    }

break_while:
    if (!errno) {
        context.response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
        context.response->success = malloc(sizeof(ResponseSuccess));
        *(context.response->success) = (ResponseSuccess) RESPONSE_SUCCESS__INIT;
        context.response->success->value_case = RESPONSE_SUCCESS__VALUE_AMOUNT;
        context.response->success->amount = changed;
    }

    result = !errno;

    match_iterator_destroy(iterator);

free_indexes:
    free(indexes);

free_is_edge:
    free(is_edge);

    return result;
}

static bool handle_delete_request(const RequestDeleteOp * request, struct request_context context) {
    if (!context.match_definition) {
        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
        context.response->error = "you cannot delete without match";
        return true;
    }

    bool * const is_edge = malloc(sizeof(bool) * request->n_names);
    if (!is_edge) {
        return false;
    }

    bool result = true;

    size_t * const indexes = malloc(sizeof(size_t) * request->n_names);
    if (!indexes) {
        result = false;
        goto free_is_edge;
    }

    for (size_t i = 0; i < request->n_names; ++i) {
        const char * const deletion = request->names[i];

        bool found = false;
        for (size_t j = 0; j < context.match_definition->vertices.amount; ++j) {
            const char * name = context.match_definition->vertices.vertices[j].name;

            if (name && strcmp(name, deletion) == 0) {
                is_edge[i] = false;
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (found) {
            continue;
        }

        for (size_t j = 0; j < context.match_definition->edges.amount; ++j) {
            const char * name = context.match_definition->edges.edges[j].name;

            if (name && strcmp(name, deletion) == 0) {
                is_edge[i] = true;
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (!found) {
            context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
            context.response->error = "you cannot delete undefined names";
            goto free_indexes;
        }
    }

    struct match_iterator iterator;
    if (!match_iterator_init(context.storage, &iterator, context.match_definition)) {
        result = false;
        goto free_indexes;
    }

    // WARNING: iteration works ONLY because of the data doesn't actually remove from file!

    uint64_t deleted = 0;
    while (match_iterator_next_where(&iterator, context.where)) {
        for (size_t i = 0; i < request->n_names; ++i) {
            if (is_edge[i]) {
                if (!storage_edge_drop(iterator.edges[indexes[i]])) {
                    result = false;
                    break;
                }
            } else {
                struct storage_edge edge;

                if (!storage_get_first_edge(iterator.vertices[indexes[i]].pointer.storage, &edge)) {
                    return false;
                }

                while (edge.pointer.offset) {
                    struct storage_vertex v;

                    if (!storage_edge_get_source(edge, &v)) {
                        return false;
                    }

                    if (storage_vertex_equals(v, iterator.vertices[indexes[i]])) {
                        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
                        context.response->error = "you cannot delete vertices with incident edges";
                        return true;
                    }

                    if (!storage_edge_get_destination(edge, &v)) {
                        return false;
                    }

                    if (storage_vertex_equals(v, iterator.vertices[indexes[i]])) {
                        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
                        context.response->error = "you cannot delete vertices with incident edges";
                        return true;
                    }

                    if (!storage_edge_next(&edge)) {
                        return false;
                    }
                }

                if (!storage_vertex_drop(iterator.vertices[indexes[i]])) {
                    result = false;
                    break;
                }
            }

            ++deleted;
        }

        if (errno) {
            break;
        }
    }

    if (!errno) {
        context.response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
        context.response->success = malloc(sizeof(ResponseSuccess));
        *(context.response->success) = (ResponseSuccess) RESPONSE_SUCCESS__INIT;
        context.response->success->value_case = RESPONSE_SUCCESS__VALUE_AMOUNT;
        context.response->success->amount = deleted;
    }

    result = !errno;

    match_iterator_destroy(iterator);

free_indexes:
    free(indexes);

free_is_edge:
    free(is_edge);

    return result;
}

static bool load_vertex_entity(struct storage_vertex vertex, VertexEntity ** vertex_entity_ptr) {
    VertexEntity * const vertex_entity = *vertex_entity_ptr = malloc(sizeof(VertexEntity));

    if (!vertex_entity) {
        return false;
    }

    *vertex_entity = (VertexEntity) VERTEX_ENTITY__INIT;

    size_t labels_amount, attributes_amount;
    if (!storage_vertex_get_labels_amount(vertex, &labels_amount)) {
        return false;
    }

    if (!storage_vertex_get_attributes_amount(vertex, &attributes_amount)) {
        return false;
    }

    vertex_entity->n_labels = labels_amount;
    if (labels_amount > 0) {
        vertex_entity->labels = calloc(labels_amount, sizeof(char *));

        if (!vertex_entity->labels) {
            goto free_vertex_entity;
        }
    } else {
        vertex_entity->labels = NULL;
    }

    vertex_entity->n_attrs = attributes_amount;
    if (attributes_amount > 0) {
        vertex_entity->attrs = calloc(attributes_amount, sizeof(Attribute *));

        if (!vertex_entity->attrs) {
            goto free_labels;
        }
    } else {
        vertex_entity->attrs = NULL;
    }

    if (labels_amount > 0) {
        struct storage_label label;

        if (!storage_vertex_get_first_label(vertex, &label)) {
            goto free_attrs;
        }

        for (size_t i = 0; i < labels_amount; ++i) {
            if (!storage_label_get(label, &(vertex_entity->labels[i]))) {
                goto free_labels_contents;
            }

            if (!storage_label_next(&label)) {
                goto free_labels_contents;
            }
        }
    }

    if (attributes_amount > 0) {
        struct storage_attribute attribute;

        if (!storage_vertex_get_first_attribute(vertex, &attribute)) {
            goto free_labels_contents;
        }

        for (size_t i = 0; i < attributes_amount; ++i) {
            Attribute * const attr = vertex_entity->attrs[i] = malloc(sizeof(Attribute));

            if (!attr) {
                goto free_labels_contents;
            }

            *attr = (Attribute) ATTRIBUTE__INIT;

            if (!storage_attribute_get(attribute, &(attr->name), &(attr->value))) {
                goto free_attributes_contents;
            }

            if (!storage_attribute_next(&attribute)) {
                goto free_attributes_contents;
            }
        }
    }

    return true;

free_attributes_contents:
    for (size_t i = 0; i < attributes_amount; ++i) {
        if (!vertex_entity->attrs[i]) {
            break;
        }

        free(vertex_entity->attrs[i]->name);
        free(vertex_entity->attrs[i]->value);
        free(vertex_entity->attrs[i]);
    }

free_labels_contents:
    for (size_t i = 0; i < labels_amount; ++i) {
        if (!vertex_entity->labels[i]) {
            break;
        }

        free(vertex_entity->labels[i]);
    }

free_attrs:
    free(vertex_entity->attrs);

free_labels:
    free(vertex_entity->labels);

free_vertex_entity:
    free(vertex_entity);
    return false;
}

static void free_vertex_entity(VertexEntity * vertex) {
    if (!vertex) {
        return;
    }

    for (size_t i = 0; i < vertex->n_labels; ++i) {
        free(vertex->labels[i]);
    }

    for (size_t i = 0; i < vertex->n_attrs; ++i) {
        Attribute * const attr = vertex->attrs[i];

        if (!attr) {
            continue;
        }

        free(attr->name);
        free(attr->value);
        free(attr);
    }

    free(vertex->labels);
    free(vertex->attrs);
}

static bool handle_return_request(const RequestReturnOp * request, struct request_context context) {
    if (!context.match_definition) {
        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
        context.response->error = "you cannot return without match";
        return true;
    }

    const uint64_t limit = request->has_limit ? request->limit : LIMIT_DEFAULT;
    if (limit > LIMIT_MAX) {
        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
        context.response->error = "limit value exceeded max value " STRINGIFY_VALUE(LIMIT_MAX);
        return true;
    }

    const uint64_t skip = request->has_skip ? request->skip : 0;

    size_t * const indexes = malloc(sizeof(size_t) * request->n_values);
    if (!indexes) {
        return false;
    }

    bool result = true;
    for (size_t i = 0; i < request->n_values; ++i) {
        const char * const value_name = request->values[i]->name;

        bool found = false;
        for (size_t j = 0; j < context.match_definition->vertices.amount; ++j) {
            const char * name = context.match_definition->vertices.vertices[j].name;

            if (name && strcmp(name, value_name) == 0) {
                indexes[i] = j;
                found = true;
                break;
            }
        }

        if (found) {
            continue;
        }

        context.response->payload_case = RESPONSE__PAYLOAD_ERROR;
        context.response->error = "you can return only vertices and they attributes";
        goto free_indexes;
    }

    const size_t n_columns = request->n_values;
    char ** const columns = calloc(n_columns, sizeof(char *));
    if (!columns) {
        result = false;
        goto free_indexes;
    }

    for (size_t i = 0; i < request->n_values; ++i) {
        const RequestReturnOpValue * value = request->values[i];

        if (!value->attr) {
            columns[i] = strdup(value->name);

            if (!columns[i]) {
                goto free_columns;
            }

            continue;
        }

        const size_t length = strlen(value->name) + 1 + strlen(value->attr) + 1;
        columns[i] = malloc(sizeof(char) * length);

        if (!columns[i]) {
            goto free_columns;
        }

        snprintf(columns[i], length, "%s.%s", value->name, value->attr);
    }

    struct match_iterator iterator;
    if (!match_iterator_init(context.storage, &iterator, context.match_definition)) {
        result = false;
        goto free_columns;
    }

    ResponseSuccessRow ** const rows = calloc(limit, sizeof(ResponseSuccessRow));
    if (!rows) {
        result = false;
        goto free_columns;
    }

    uint64_t got_rows = 0;
    uint64_t to_skip = skip;
    while (match_iterator_next_where(&iterator, context.where)) {
        if (to_skip > 0) {
            --to_skip;
            continue;
        }

        if (got_rows == limit) {
            break;
        }

        ResponseSuccessRow * const row = rows[got_rows] = malloc(sizeof(ResponseSuccessRow));
        if (!row) {
            result = false;
            goto free_rows;
        }

        *row = (ResponseSuccessRow) RESPONSE_SUCCESS_ROW__INIT;

        row->n_cells = n_columns;
        row->cells = calloc(n_columns, sizeof(ResponseSuccessCell *));
        if (!row->cells) {
            result = false;
            goto free_rows;
        }

        for (size_t i = 0; i < n_columns; ++i) {
            row->cells[i] = malloc(sizeof(ResponseSuccessCell));

            if (!row->cells[i]) {
                result = false;
                goto free_rows;
            }

            *(row->cells[i]) = (ResponseSuccessCell) RESPONSE_SUCCESS_CELL__INIT;
        }

        for (size_t i = 0; i < n_columns; ++i) {
            const RequestReturnOpValue * value = request->values[i];
            ResponseSuccessCell * const cell = row->cells[i];

            if (value->attr) {
                if (!storage_vertex_get_attribute(iterator.vertices[indexes[i]], value->attr, &(cell->string))) {
                    result = false;
                    goto free_rows;
                }

                if (cell->string) {
                    cell->value_case = RESPONSE_SUCCESS_CELL__VALUE_STRING;
                }
            } else {
                if (!load_vertex_entity(iterator.vertices[indexes[i]], &(cell->vertex))) {
                    result = false;
                    goto free_rows;
                }

                cell->vertex->name = value->name;
                cell->value_case = RESPONSE_SUCCESS_CELL__VALUE_VERTEX;
            }
        }

        ++got_rows;
    }

    if (!errno) {
        context.response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
        context.response->success = malloc(sizeof(ResponseSuccess));
        *(context.response->success) = (ResponseSuccess) RESPONSE_SUCCESS__INIT;
        context.response->success->value_case = RESPONSE_SUCCESS__VALUE_TABLE;
        context.response->success->table = malloc(sizeof(ResponseSuccessTable));
        *(context.response->success->table) = (ResponseSuccessTable) RESPONSE_SUCCESS_TABLE__INIT;
        context.response->success->table->n_columns = n_columns;
        context.response->success->table->columns = columns;
        context.response->success->table->n_rows = got_rows;
        context.response->success->table->rows = rows;
    }

    result = !errno;

    if (result) {
        match_iterator_destroy(iterator);
        goto free_indexes;
    }

free_rows:
    for (size_t i_row = 0; i_row < limit; ++i_row) {
        ResponseSuccessRow * const row = rows[i_row];

        if (!row) {
            break;
        }

        for (size_t i_cell = 0; i_cell < n_columns; ++i_cell) {
            ResponseSuccessCell * const cell = row->cells[i_cell];

            if (!cell) {
                break;
            }

            switch (cell->value_case) {
                case RESPONSE_SUCCESS_CELL__VALUE_STRING:
                    free(cell->string);
                    break;

                case RESPONSE_SUCCESS_CELL__VALUE_VERTEX:
                    free_vertex_entity(cell->vertex);
                    break;

                default:
                    ; // do nothing
            }

            free(cell);
        }

        free(row->cells);
        free(row);
    }

    free(rows);

// free_iterator:
    match_iterator_destroy(iterator);

free_columns:
    for (size_t i = 0; i < n_columns; ++i) {
        if (!columns[i]) {
            break;
        }

        free(columns[i]);
    }

    free(columns);

free_indexes:
    free(indexes);

    return result;
}

static bool handle_request(const Request * request, storage storage, Response * response) {
    struct match_iterator_definition definition;

    if (!create_match_definition(request->n_match, request->match, &definition, response)) {
        return !errno;
    }

    const struct match_iterator_definition * definition_pointer = &definition;
    if (definition.vertices.amount == 0 && definition.edges.amount == 0) {
        definition_pointer = NULL;
    }

    const char * where_error;
    if (!check_where(definition_pointer, request->where, &where_error)) {
        response->payload_case = RESPONSE__PAYLOAD_ERROR;
        response->error = (char *) where_error;
        return true;
    }

    const struct request_context context = {
        .where = request->where,
        .storage = storage,
        .match_definition = definition_pointer,
        .response = response,
    };

    bool ret;
    switch (request->op_case) {
        case REQUEST__OP_CREATE:
            ret = handle_create_request(request->create, context);
            break;

        case REQUEST__OP_SET:
            ret = handle_set_request(request->set, context);
            break;

        case REQUEST__OP_REMOVE:
            ret = handle_remove_request(request->remove, context);
            break;

        case REQUEST__OP_DELETE:
            ret = handle_delete_request(request->delete_, context);
            break;

        case REQUEST__OP_RETURN:
            ret = handle_return_request(request->return_, context);
            break;

        default:
            response->payload_case = RESPONSE__PAYLOAD_ERROR;
            response->error = "bad request";
            ret = true;
    }

    match_iterator_definition_destroy(definition);
    return ret;
}

static void handle_client(int socket, storage storage) {
    printf("Connected\n");

    while (!closing) {
        uint32_t request_size;

        if (!read_full_value(socket, &request_size)) {
            if (errno == EPIPE || errno == ECONNRESET) {
                errno = 0;
            }

            break;
        }

        request_size = ntohl(request_size);
        if (request_size == 0) {
            continue;
        }

        uint8_t * const request_buffer = malloc(request_size);
        if (!request_buffer) {
            break;
        }

        if (!read_full(socket, request_buffer, request_size)) {
            break;
        }

        Request * const request = request__unpack(NULL, request_size, request_buffer);
        if (!request) {
            printf("An error occurred while request receiving.\n");
            free(request_buffer);
            continue;
        }

        free(request_buffer);
        printf("Received request of %"PRIu32" bytes.\n", request_size);

        Response response = RESPONSE__INIT;
        if (!handle_request(request, storage, &response)) {
            request__free_unpacked(request, NULL);
            break;
        }

        size_t response_size = response__get_packed_size(&response);
        if ((int64_t) response_size > (int64_t) UINT32_MAX) {
            printf("Response is too long: %zu bytes.\n", response_size);

            response_size = 0;
        }

        uint8_t * const response_buffer = malloc(response_size);
        if (!response_buffer && response_size > 0) {
            request__free_unpacked(request, NULL);
            break;
        }

        if (response_buffer) {
            response_size = response__pack(&response, response_buffer);
        }

        request__free_unpacked(request, NULL);

        const uint32_t response_size_n = htonl((uint32_t) response_size);
        if (!write_full_value(socket, &response_size_n)) {
            free(response_buffer);
            request__free_unpacked(request, NULL);
            break;
        }

        if (response_buffer) {
            if (!write_full(socket, response_buffer, response_size)) {
                free(response_buffer);
                break;
            }

            printf("Sent response of %zu bytes.\n", response_size);
            // TODO free response with table
            free(response_buffer);
        }
    }

    if (errno) {
        perror("Error while handling client");
        errno = 0;
    }

    close(socket);
    printf("Disconnected\n");
}

int main(int argc, char * argv[]) {
    if (argc < 2) {
        return EINVAL;
    }

    int fd = open(argv[1], O_RDWR);
    storage storage;

    if (fd < 0 && errno != ENOENT) {
        perror("Error while opening file");
        return errno;
    }

    if (fd < 0 && errno == ENOENT) {
        fd = open(argv[1], O_CREAT | O_RDWR, 0644);

        if (!storage_init(fd, &storage)) {
            perror("Could not init storage");
            return errno;
        }
    } else {
        if (!storage_open(fd, &storage)) {
            perror("Could not open storage");
            return errno;
        }
    }

    // create the server socket
    const int server_socket = socket(AF_INET, SOCK_STREAM, 0);

    // define the server address
    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(9002);
    server_address.sin_addr.s_addr = INADDR_ANY;

    // bind the socket to our specified IP and port
    if (bind(server_socket, (struct sockaddr *) &server_address, sizeof(server_address)) != 0) {
        perror("Cannot start server");
        return 0;
    }

    // second argument is a backlog - how many connections can be waiting for this socket simultaneously
    listen(server_socket, 1);

    {
        struct sigaction sa;

        sa.sa_sigaction = close_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_SIGINFO;

        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
    }

    printf("Server started.\n");

    while (!closing) {
        int ret = accept(server_socket, NULL, NULL);

        if (ret < 0) {
            break;
        }

        handle_client(ret, storage);
    }

    close(server_socket);
    close(fd);

    printf("Bye!\n");
    return 0;
}
