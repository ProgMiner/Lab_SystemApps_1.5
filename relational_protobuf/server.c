#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <stdbool.h>
#include <signal.h>

#include "api.pb-c.h"
#include "storage.h"
#include "utils.h"


static volatile bool closing = false;

static void close_handler(int sig, siginfo_t * info, void * context) {
    closing = true;
}

static void make_error_response(const char * error, Response * response) {
    response->payload_case = RESPONSE__PAYLOAD_ERROR;
    response->error = strdup(error);
}

static SuccessResponse * make_success_response(Response * response) {
    SuccessResponse * const success_response = malloc(sizeof(*success_response));

    success_response__init(success_response);
    response->payload_case = RESPONSE__PAYLOAD_SUCCESS;
    response->success = success_response;

    return success_response;
}

static void make_success_amount_response(uint64_t amount, Response * response) {
    SuccessResponse * const success_response = make_success_response(response);

    success_response->value_case = SUCCESS_RESPONSE__VALUE_AMOUNT;
    success_response->amount = amount;
}

static const char * print_Value_type(const Value * value) {
    switch (value->value_case) {
        case VALUE__VALUE__NOT_SET:
            return "NULL";

        case VALUE__VALUE_INT:
            return "int";

        case VALUE__VALUE_UINT:
            return "uint";

        case VALUE__VALUE_NUM:
            return "num";

        case VALUE__VALUE_STR:
            return "str";

        default:
            return NULL;
    }
}

static Value * make_Value_from_value(struct storage_value * value) {
    Value * const result = malloc(sizeof(Value));

    value__init(result);

    if (!value) {
        result->value_case = VALUE__VALUE__NOT_SET;
        return result;
    }

    switch (value->type) {
        case STORAGE_COLUMN_TYPE_INT:
            result->value_case = VALUE__VALUE_INT;
            result->int_ = value->value._int;
            break;

        case STORAGE_COLUMN_TYPE_UINT:
            result->value_case = VALUE__VALUE_UINT;
            result->uint = value->value.uint;
            break;

        case STORAGE_COLUMN_TYPE_NUM:
            result->value_case = VALUE__VALUE_NUM;
            result->num = value->value.num;
            break;

        case STORAGE_COLUMN_TYPE_STR:
            result->value_case = VALUE__VALUE_STR;
            result->str = strdup(value->value.str);
            break;
    }

    storage_value_delete(value);
    return result;
}

static const struct storage_value * make_value_from_Value(const Value * value, struct storage_value * container) {
    switch (value->value_case) {
        case VALUE__VALUE_INT:
            container->type = STORAGE_COLUMN_TYPE_INT;
            container->value._int = value->int_;
            break;

        case VALUE__VALUE_UINT:
            container->type = STORAGE_COLUMN_TYPE_UINT;
            container->value.uint = value->uint;
            break;

        case VALUE__VALUE_NUM:
            container->type = STORAGE_COLUMN_TYPE_NUM;
            container->value.num = value->num;
            break;

        case VALUE__VALUE_STR:
            container->type = STORAGE_COLUMN_TYPE_STR;
            container->value.str = strdup(value->str);
            break;

        default:
            return NULL;
    }

    return container;
}

static void handle_request_create_table(const CreateTableRequest * request, struct storage * storage, Response * response) {
    struct storage_table * table = malloc(sizeof(*table));

    table->storage = storage;
    table->position = 0;
    table->next = 0;
    table->first_row = 0;
    table->name = strdup(request->table);
    table->columns.amount = request->n_columns;
    table->columns.columns = malloc(sizeof(*table->columns.columns) * request->n_columns);

    for (int i = 0; i < request->n_columns; ++i) {
        table->columns.columns[i].name = strdup(request->columns[i]->name);
        table->columns.columns[i].type = (enum storage_column_type) request->columns[i]->type;
    }

    errno = 0;
    storage_table_add(table);
    bool error = errno != 0;

    storage_table_delete(table);

    if (error) {
        make_error_response("a table with the same name is already exists", response);
    } else {
        make_success_response(response);
    }
}

static void handle_request_drop_table(const DropTableRequest * request, struct storage * storage, Response * response) {
    struct storage_table * table = storage_find_table(storage, request->table);

    if (!table) {
        make_error_response("table with the specified name is not exists", response);
        return;
    }

    storage_table_remove(table);
    storage_table_delete(table);
    make_success_response(response);
}

static bool map_columns_to_indexes(unsigned int request_columns_amount, char ** request_columns_names,
    struct storage_joined_table * table, unsigned int * columns_amount, unsigned int ** columns_indexes, Response * response) {
    unsigned int columns_count = request_columns_amount;

    const uint16_t table_columns_amount = storage_joined_table_get_columns_amount(table);

    if (columns_count == 0) {
        columns_count = table_columns_amount;
    }

    *columns_indexes = malloc(sizeof(**columns_indexes) * columns_count);
    if (request_columns_amount == 0) {
        for (unsigned int i = 0; i < columns_count; ++i) {
            (*columns_indexes)[i] = i;
        }
    } else {
        for (unsigned int i = 0; i < columns_count; ++i) {
            bool found = false;

            for (unsigned int j = 0; j < table_columns_amount; ++j) {
                if (strcmp(request_columns_names[i], storage_joined_table_get_column(table, j).name) == 0) {
                    (*columns_indexes)[i] = j;
                    found = true;
                    break;
                }
            }

            if (!found) {
                size_t msg_length = 41 + strlen(request_columns_names[i]);

                char msg[msg_length];
                snprintf(msg, msg_length, "column with name %s is not exists in table", request_columns_names[i]);

                make_error_response(msg, response);
                return false;
            }
        }
    }

    *columns_amount = columns_count;
    return true;
}

static bool check_values(unsigned int request_values_amount, Value ** request_values_values, struct storage_table * table,
    unsigned int columns_amount, const unsigned int * columns_indexes, Response * response) {

    if (request_values_amount != columns_amount) {
        make_error_response("values amount is not equals to columns amount", response);
        return false;
    }

    for (unsigned int i = 0; i < columns_amount; ++i) {
        if (request_values_values[i]->value_case == VALUE__VALUE__NOT_SET) {
            continue;
        }

        struct storage_column column = table->columns.columns[columns_indexes[i]];

        switch (column.type) {
            case STORAGE_COLUMN_TYPE_INT:
                if (request_values_values[i]->value_case == VALUE__VALUE_INT) {
                    continue;
                }
                break;

            case STORAGE_COLUMN_TYPE_UINT:
                if (request_values_values[i]->value_case == VALUE__VALUE_UINT) {
                    continue;
                }
                break;

            case STORAGE_COLUMN_TYPE_NUM:
                if (request_values_values[i]->value_case == VALUE__VALUE_NUM) {
                    continue;
                }
                break;

            case STORAGE_COLUMN_TYPE_STR:
                if (request_values_values[i]->value_case == VALUE__VALUE_STR) {
                    continue;
                }
                break;
        }

        switch (request_values_values[i]->value_case) {
            case VALUE__VALUE_INT:
                if (column.type == STORAGE_COLUMN_TYPE_UINT) {
                    if (request_values_values[i]->int_ >= 0) {
                        request_values_values[i]->value_case = VALUE__VALUE_UINT;
                        request_values_values[i]->uint = (uint64_t) request_values_values[i]->int_;
                        continue;
                    }
                }

                break;

            case VALUE__VALUE_UINT:
                if (column.type == STORAGE_COLUMN_TYPE_INT) {
                    if (request_values_values[i]->uint <= INT64_MAX) {
                        request_values_values[i]->value_case = VALUE__VALUE_INT;
                        request_values_values[i]->int_ = (int64_t) request_values_values[i]->uint;
                        continue;
                    }
                }

                break;

            default:
                break;
        }

        const char * col_type = storage_column_type_to_string(column.type);
        const char * val_type = print_Value_type(request_values_values[i]);
        size_t msg_length = 47 + strlen(column.name) + strlen(col_type) + strlen(val_type);

        char msg[msg_length];
        snprintf(msg, msg_length, "value for column with name %s (%s) has wrong type %s", column.name, col_type, val_type);

        make_error_response(msg, response);
        return false;
    }

    return true;
}

static void handle_request_insert(const InsertRequest * request, struct storage * storage, Response * response) {
    struct storage_table * table = storage_find_table(storage, request->table);

    if (!table) {
        make_error_response("table with the specified name is not exists", response);
        return;
    }

    unsigned int columns_amount;
    unsigned int * columns_indexes;
    struct storage_joined_table * joined_table = storage_joined_table_wrap(table);

    if (!map_columns_to_indexes(request->n_columns, request->columns, joined_table, &columns_amount, &columns_indexes, response)) {
        storage_joined_table_delete(joined_table);
        return;
    }

    if (!check_values(request->n_values, request->values, table, columns_amount, columns_indexes, response)) {
        free(columns_indexes);
        storage_joined_table_delete(joined_table);
        return;
    }

    struct storage_row * row = storage_table_add_row(table);
    for (unsigned int i = 0; i < columns_amount; ++i) {
        struct storage_value value;

        storage_row_set_value(row, columns_indexes[i], make_value_from_Value(request->values[i], &value));

        storage_value_destroy(value);
    }

    free(columns_indexes);
    storage_row_delete(row);
    storage_joined_table_delete(joined_table);

    make_success_response(response);
}

static bool is_where_correct(const struct storage_joined_table * table, const WhereExpr * where, Response * response) {
    if (!where) {
        return true;
    }

    const uint16_t table_columns_amount = storage_joined_table_get_columns_amount(table);
    const WhereValueOp * where_value_op;
    const WhereExprOp * where_expr_op;

    switch (where->op_case) {
        case WHERE_EXPR__OP_EQ:
            where_value_op = where->eq;
            break;

        case WHERE_EXPR__OP_NE:
            where_value_op = where->ne;
            break;

        case WHERE_EXPR__OP_LT:
            where_value_op = where->lt;
            break;

        case WHERE_EXPR__OP_GT:
            where_value_op = where->gt;
            break;

        case WHERE_EXPR__OP_LE:
            where_value_op = where->le;
            break;

        case WHERE_EXPR__OP_GE:
            where_value_op = where->ge;
            break;

        case WHERE_EXPR__OP_AND:
            where_expr_op = where->and_;
            break;

        case WHERE_EXPR__OP_OR:
            where_expr_op = where->or_;
            break;

        default:
            make_error_response("bad request", response);
            return false;
    }

    switch (where->op_case) {
        case WHERE_EXPR__OP_EQ:
        case WHERE_EXPR__OP_NE:
            if (where_value_op->value->value_case == VALUE__VALUE__NOT_SET) {
                return true;
            }

        case WHERE_EXPR__OP_LT:
        case WHERE_EXPR__OP_GT:
        case WHERE_EXPR__OP_LE:
        case WHERE_EXPR__OP_GE:
            if (where_value_op->value->value_case == VALUE__VALUE__NOT_SET) {
                make_error_response("NULL value is not comparable", response);
                return false;
            }

            for (unsigned int i = 0; i < table_columns_amount; ++i) {
                struct storage_column column = storage_joined_table_get_column(table, i);

                if (strcmp(column.name, where_value_op->column) == 0) {
                    switch (column.type) {
                        case STORAGE_COLUMN_TYPE_INT:
                        case STORAGE_COLUMN_TYPE_UINT:
                        case STORAGE_COLUMN_TYPE_NUM:
                            switch (where_value_op->value->value_case) {
                                case VALUE__VALUE_INT:
                                case VALUE__VALUE_UINT:
                                case VALUE__VALUE_NUM:
                                    return true;

                                default:
                                    break;
                            }

                            break;

                        case STORAGE_COLUMN_TYPE_STR:
                            if (where_value_op->value->value_case == VALUE__VALUE_STR) {
                                return true;
                            }

                            break;
                    }

                    const char * const column_type = storage_column_type_to_string(column.type);
                    const char * const value_type = print_Value_type(where_value_op->value);
                    const size_t msg_length = 31 + strlen(column_type) + strlen(value_type);

                    char msg[msg_length];
                    snprintf(msg, msg_length, "types %s and %s are not comparable", column_type, value_type);
                    make_error_response(msg, response);
                    return false;
                }
            }

            {
                const size_t msg_length = 41 + strlen(where_value_op->column);

                char msg[msg_length];
                snprintf(msg, msg_length, "column with name %s is not exists in table", where_value_op->column);
                make_error_response(msg, response);
                return false;
            }

        case WHERE_EXPR__OP_AND:
        case WHERE_EXPR__OP_OR:
            return is_where_correct(table, where_expr_op->left, response)
                && is_where_correct(table, where_expr_op->right, response);

        default:
            return false; // unreachable
    }
}

static bool compare_values_not_null(WhereExpr__OpCase op, struct storage_value left, const Value * right) {
    switch (op) {
        case WHERE_EXPR__OP_EQ:
            switch (left.type) {
                case STORAGE_COLUMN_TYPE_INT:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            return left.value._int == right->int_;

                        case VALUE__VALUE_UINT:
                            if (left.value._int < 0) {
                                return false;
                            }

                            return ((uint64_t) left.value._int) == right->uint;

                        case VALUE__VALUE_NUM:
                            return ((double) left.value._int) == right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_UINT:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            if (right->int_ < 0) {
                                return false;
                            }

                            return left.value.uint == ((uint64_t) right->int_);

                        case VALUE__VALUE_UINT:
                            return left.value.uint == right->uint;

                        case VALUE__VALUE_NUM:
                            return ((double) left.value.uint) == right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_NUM:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            return left.value.num == ((double) right->int_);

                        case VALUE__VALUE_UINT:
                            return left.value.num == ((double) right->uint);

                        case VALUE__VALUE_NUM:
                            return left.value.num == right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_STR:
                    if (right->value_case == VALUE__VALUE_STR) {
                        return strcmp(left.value.str, right->str) == 0;
                    } else {
                        return false;
                    }
            }

        case WHERE_EXPR__OP_NE:
            return !compare_values_not_null(WHERE_EXPR__OP_EQ, left, right);

        case WHERE_EXPR__OP_LT:
            switch (left.type) {
                case STORAGE_COLUMN_TYPE_INT:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            return left.value._int < right->int_;

                        case VALUE__VALUE_UINT:
                            if (left.value._int < 0) {
                                return true;
                            }

                            return ((uint64_t) left.value._int) < right->uint;

                        case VALUE__VALUE_NUM:
                            return ((double) left.value._int) < right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_UINT:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            if (right->int_ < 0) {
                                return false;
                            }

                            return left.value.uint < ((uint64_t) right->int_);

                        case VALUE__VALUE_UINT:
                            return left.value.uint < right->uint;

                        case VALUE__VALUE_NUM:
                            return ((double) left.value.uint) < right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_NUM:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            return left.value.num < ((double) right->int_);

                        case VALUE__VALUE_UINT:
                            return left.value.num < ((double) right->uint);

                        case VALUE__VALUE_NUM:
                            return left.value.num < right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_STR:
                    if (right->value_case == VALUE__VALUE_STR) {
                        return strcmp(left.value.str, right->str) < 0;
                    } else {
                        return false;
                    }

            }

        case WHERE_EXPR__OP_GT:
            switch (left.type) {
                case STORAGE_COLUMN_TYPE_INT:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            return left.value._int > right->int_;

                        case VALUE__VALUE_UINT:
                            if (left.value._int < 0) {
                                return false;
                            }

                            return ((uint64_t) left.value._int) > right->uint;

                        case VALUE__VALUE_NUM:
                            return ((double) left.value._int) > right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_UINT:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            if (right->int_ < 0) {
                                return true;
                            }

                            return left.value.uint > ((uint64_t) right->int_);

                        case VALUE__VALUE_UINT:
                            return left.value.uint > right->uint;

                        case VALUE__VALUE_NUM:
                            return ((double) left.value.uint) > right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_NUM:
                    switch (right->value_case) {
                        case VALUE__VALUE_INT:
                            return left.value.num > ((double) right->int_);

                        case VALUE__VALUE_UINT:
                            return left.value.num > ((double) right->uint);

                        case VALUE__VALUE_NUM:
                            return left.value.num > right->num;

                        default:
                            return false;
                    }

                case STORAGE_COLUMN_TYPE_STR:
                    if (right->value_case == VALUE__VALUE_STR) {
                        return strcmp(left.value.str, right->str) > 0;
                    } else {
                        return false;
                    }
            }

        case WHERE_EXPR__OP_LE:
            return !compare_values_not_null(WHERE_EXPR__OP_GT, left, right);

        case WHERE_EXPR__OP_GE:
            return !compare_values_not_null(WHERE_EXPR__OP_LT, left, right);

        default:
            return false;
    }
}

static bool compare_values(WhereExpr__OpCase op, struct storage_value * left, const Value * right) {
    switch (op) {
        case WHERE_EXPR__OP_EQ:
            if (left == NULL || right->value_case == VALUE__VALUE__NOT_SET) {
                return left == NULL && right->value_case == VALUE__VALUE__NOT_SET;
            }

            break;

        case WHERE_EXPR__OP_NE:
            if (left == NULL || right->value_case == VALUE__VALUE__NOT_SET) {
                return (left == NULL) != (right->value_case == VALUE__VALUE__NOT_SET);
            }

            break;

        case WHERE_EXPR__OP_LT:
        case WHERE_EXPR__OP_GT:
        case WHERE_EXPR__OP_LE:
        case WHERE_EXPR__OP_GE:
            if (left == NULL || right->value_case == VALUE__VALUE__NOT_SET) {
                return false;
            }

            break;

        default:
            return false;
    }

    return compare_values_not_null(op, *left, right);
}

static bool eval_where(const struct storage_joined_row * row, const WhereExpr * where) {
    if (!where) {
        return true;
    }

    const uint16_t table_columns_amount = storage_joined_table_get_columns_amount(row->table);

    const WhereValueOp *  where_value_op;
    switch (where->op_case) {
        case WHERE_EXPR__OP_EQ:
            where_value_op = where->eq;
            break;

        case WHERE_EXPR__OP_NE:
            where_value_op = where->ne;
            break;

        case WHERE_EXPR__OP_LT:
            where_value_op = where->lt;
            break;

        case WHERE_EXPR__OP_GT:
            where_value_op = where->gt;
            break;

        case WHERE_EXPR__OP_LE:
            where_value_op = where->le;
            break;

        case WHERE_EXPR__OP_GE:
            where_value_op = where->ge;
            break;

        case WHERE_EXPR__OP_AND:
        case WHERE_EXPR__OP_OR:
            break;

        default:
            errno = EINVAL;
            return false;
    }

    switch (where->op_case) {
        case WHERE_EXPR__OP_EQ:
        case WHERE_EXPR__OP_NE:
        case WHERE_EXPR__OP_LT:
        case WHERE_EXPR__OP_GT:
        case WHERE_EXPR__OP_LE:
        case WHERE_EXPR__OP_GE:
            for (unsigned int i = 0; i < table_columns_amount; ++i) {
                if (strcmp(storage_joined_table_get_column(row->table, i).name, where_value_op->column) == 0) {
                    return compare_values(where->op_case, storage_joined_row_get_value(row, i), where_value_op->value);
                }
            }

            errno = EINVAL;
            return false;

        case WHERE_EXPR__OP_AND:
            return eval_where(row, where->and_->left) && eval_where(row, where->and_->right);

        case WHERE_EXPR__OP_OR:
            return eval_where(row, where->or_->left) || eval_where(row, where->or_->right);

        default:
            return false; // unreachable
    }
}

static void handle_request_delete(const DeleteRequest * request, struct storage * storage, Response * response) {
    struct storage_table * const table = storage_find_table(storage, request->table);

    if (!table) {
        make_error_response("table with the specified name is not exists", response);
        return;
    }

    struct storage_joined_table * joined_table = storage_joined_table_wrap(table);

    if (!is_where_correct(joined_table, request->where, response)) {
        storage_joined_table_delete(joined_table);
        return;
    }

    unsigned long long amount = 0;
    for (struct storage_joined_row * row = storage_joined_table_get_first_row(joined_table); row; row = storage_joined_row_next(row)) {
        if (eval_where(row, request->where)) {
            storage_row_remove(row->rows[0]);
            ++amount;
        }
    }

    storage_joined_table_delete(joined_table);
    make_success_amount_response(amount, response);
}

static void handle_request_select(const SelectRequest * request, struct storage * storage, Response * response) {
    const size_t offset = request->has_offset ? request->offset : 0;
    const size_t limit = request->has_limit ? request->limit : 10;

    if (limit > 1000) {
        make_error_response("limit is too high", response);
        return;
    }

    struct storage_table * table = storage_find_table(storage, request->table);

    if (!table) {
        make_error_response("table with the specified name is not exists", response);
        return;
    }

    struct storage_joined_table * joined_table = storage_joined_table_new(request->n_joins + 1);
    joined_table->tables.tables[0].table = table;
    joined_table->tables.tables[0].t_column_index = 0;
    joined_table->tables.tables[0].s_column_index = 0;

    for (int i = 0; i < request->n_joins; ++i) {
        joined_table->tables.tables[i + 1].table = storage_find_table(storage, request->joins[i]->table);

        if (!joined_table->tables.tables[i + 1].table) {
            storage_joined_table_delete(joined_table);

            make_error_response("table with the specified name is not exists", response);
            return;
        }

        joined_table->tables.tables[i + 1].t_column_index = (uint16_t) -1;
        for (int j = 0; j < joined_table->tables.tables[i + 1].table->columns.amount; ++j) {
            if (strcmp(request->joins[i]->t_column, joined_table->tables.tables[i + 1].table->columns.columns[j].name) == 0) {
                joined_table->tables.tables[i + 1].t_column_index = j;
                break;
            }
        }

        if (joined_table->tables.tables[i + 1].t_column_index >= joined_table->tables.tables[i + 1].table->columns.amount) {
            storage_joined_table_delete(joined_table);

            make_error_response("column with the specified name is not exists in table", response);
            return;
        }

        uint16_t slice_columns = 0;
        joined_table->tables.tables[i + 1].s_column_index = (uint16_t) -1;
        for (int tbl_index = 0, col_index = 0; tbl_index <= i; ++tbl_index) {
            for (int tbl_col_index = 0; tbl_col_index < joined_table->tables.tables[tbl_index].table->columns.amount; ++tbl_col_index, ++col_index) {
                if (strcmp(request->joins[i]->s_column, joined_table->tables.tables[tbl_index].table->columns.columns[tbl_col_index].name) == 0) {
                    joined_table->tables.tables[i + 1].s_column_index = col_index;
                    break;
                }
            }

            slice_columns += joined_table->tables.tables[tbl_index].table->columns.amount;
            if (joined_table->tables.tables[i + 1].s_column_index < slice_columns) {
                break;
            }
        }

        if (joined_table->tables.tables[i + 1].s_column_index >= slice_columns) {
            storage_joined_table_delete(joined_table);

            make_error_response("column with the specified name is not exists in the join slice", response);
            return;
        }
    }

    if (!is_where_correct(joined_table, request->where, response)) {
        storage_joined_table_delete(joined_table);
        return;
    }

    unsigned int columns_amount;
    unsigned int * columns_indexes;

    if (!map_columns_to_indexes(request->n_columns, request->columns, joined_table, &columns_amount, &columns_indexes, response)) {
        storage_joined_table_delete(joined_table);
        return;
    }

    Table * const answer = malloc(sizeof(Table));
    table__init(answer);

    {
        answer->n_columns = columns_amount;
        answer->columns = malloc(sizeof(char *) * columns_amount);

        for (unsigned int i = 0; i < columns_amount; ++i) {
            answer->columns[i] = strdup(storage_joined_table_get_column(joined_table, columns_indexes[i]).name);
        }
    }

    {
        answer->rows = malloc(sizeof(Table__Row *) * limit);

        unsigned int to_skip = offset, amount = 0;
        for (struct storage_joined_row * row = storage_joined_table_get_first_row(joined_table); row; row = storage_joined_row_next(row)) {
            if (eval_where(row, request->where)) {
                if (to_skip > 0) {
                    --to_skip;
                    continue;
                }

                if (amount == limit) {
                    break;
                }

                Table__Row * const values_row = malloc(sizeof(Table__Row));
                table__row__init(values_row);

                values_row->n_cells = columns_amount;
                values_row->cells = malloc(sizeof(Value *) * columns_amount);

                for (unsigned int i = 0; i < columns_amount; ++i) {
                    values_row->cells[i] = make_Value_from_value(storage_joined_row_get_value(row, columns_indexes[i]));
                }

                answer->rows[amount] = values_row;
                ++amount;
            }
        }

        answer->n_rows = amount;
    }

    free(columns_indexes);
    storage_joined_table_delete(joined_table);

    SuccessResponse * const success_response = make_success_response(response);
    success_response->value_case = SUCCESS_RESPONSE__VALUE_TABLE;
    success_response->table = answer;
}

static void handle_request_update(const UpdateRequest * request, struct storage * storage, Response * response) {
    struct storage_table * table = storage_find_table(storage, request->table);

    if (!table) {
        make_error_response("table with the specified name is not exists", response);
        return;
    }

    struct storage_joined_table * joined_table = storage_joined_table_wrap(table);

    if (!is_where_correct(joined_table, request->where, response)) {
        storage_joined_table_delete(joined_table);
        return;
    }

    unsigned int columns_amount;
    unsigned int * columns_indexes;

    if (!map_columns_to_indexes(request->n_columns, request->columns, joined_table, &columns_amount, &columns_indexes, response)) {
        storage_joined_table_delete(joined_table);
        return;
    }

    if (!check_values(request->n_values, request->values, table, columns_amount, columns_indexes, response)) {
        free(columns_indexes);
        storage_joined_table_delete(joined_table);
        return;
    }

    unsigned long long amount = 0;
    for (struct storage_joined_row * row = storage_joined_table_get_first_row(joined_table); row; row = storage_joined_row_next(row)) {
        if (eval_where(row, request->where)) {
            for (unsigned int i = 0; i < columns_amount; ++i) {
                struct storage_value value;

                storage_row_set_value(row->rows[0], columns_indexes[i], make_value_from_Value(request->values[i], &value));

                storage_value_destroy(value);
            }

            ++amount;
        }
    }

    free(columns_indexes);
    storage_joined_table_delete(joined_table);
    make_success_amount_response(amount, response);
}

static void handle_request(const Request * request, struct storage * storage, Response * response) {
    switch (request->action_case) {
        case REQUEST__ACTION_CREATE_TABLE:
            handle_request_create_table(request->create_table, storage, response);
            return;

        case REQUEST__ACTION_DROP_TABLE:
            handle_request_drop_table(request->drop_table, storage, response);
            return;

        case REQUEST__ACTION_INSERT:
            handle_request_insert(request->insert, storage, response);
            return;

        case REQUEST__ACTION_DELETE:
            handle_request_delete(request->delete_, storage, response);
            return;

        case REQUEST__ACTION_SELECT:
            handle_request_select(request->select, storage, response);
            return;

        case REQUEST__ACTION_UPDATE:
            handle_request_update(request->update, storage, response);
            return;

        default:
            make_error_response("bad request", response);
            return;
    }
}

static void handle_client(int socket, struct storage * storage) {
    printf("Connected\n");

    while (!closing) {
        uint32_t request_size;

        if (!read_full(socket, &request_size, sizeof(request_size))) {
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
        handle_request(request, storage, &response);
        if (response.payload_case == RESPONSE__PAYLOAD__NOT_SET) {
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
        if (!write_full(socket, &response_size_n, sizeof(response_size_n))) {
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
        return 0;
    }

    int fd = open(argv[1], O_RDWR);
    struct storage * storage;

    if (fd < 0 && errno != ENOENT) {
        perror("Error while opening file");
        return errno;
    }

    if (fd < 0 && errno == ENOENT) {
        fd = open(argv[1], O_CREAT | O_RDWR, 0644);
        storage = storage_init(fd);
    } else {
        storage = storage_open(fd);
    }

    // create the server socket
    int server_socket;
    server_socket = socket(AF_INET, SOCK_STREAM, 0);

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

    while (!closing) {
        int ret = accept(server_socket, NULL, NULL);

        if (ret < 0) {
            break;
        }

        handle_client(ret, storage);
    }

    close(server_socket);
    storage_delete(storage);
    close(fd);

    printf("Bye!\n");
    return 0;
}
