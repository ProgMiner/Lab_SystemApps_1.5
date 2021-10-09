%parse-param {Request ** result} {char ** error}

%code requires {
#include <stdbool.h>
}

%{
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include "api.pb-c.h"

int yylex(void);
void yyerror(Request ** result, char ** error, const char * str);
%}

%union {
    Attribute * attribute;
    Entity * entity;
    VertexEntity * vertex_entity;
    EdgeEntity * edge_entity;
    Request * request;
    Where * where;
    RequestCreateOp * request_create_op;
    RequestSetOp * request_set_op;
    RequestSetOpChange * request_set_op_change;
    RequestSetOpChangeAttr * request_set_op_change_attr;
    RequestRemoveOp * request_remove_op;
    RequestRemoveOpChange * request_remove_op_change;
    RequestDeleteOp * request_delete_op;
    RequestReturnOp * request_return_op;
    RequestReturnOpValue * request_return_op_value;

    struct array_Entity {
        size_t amount;
        Entity ** content;
    } array_entity;

    struct array_str {
        size_t amount;
        char ** content;
    } array_str;

    struct array_Attribute {
        size_t amount;
        Attribute ** content;
    } array_attribute;

    struct array_RequestSetOpChange {
        size_t amount;
        RequestSetOpChange ** content;
    } array_request_set_op_change;

    struct array_RequestRemoveOpChange {
        size_t amount;
        RequestRemoveOpChange ** content;
    } array_request_remove_op_change;

    struct array_RequestReturnOpValue {
        size_t amount;
        RequestReturnOpValue ** content;
    } array_request_return_op_value;

    struct oneof_Request_op {
        Request__OpCase _case;

        union {
            RequestCreateOp * create;
            RequestSetOp * set;
            RequestRemoveOp * remove;
            RequestDeleteOp * delete_;
            RequestReturnOp * return_;
        };
    } oneof_request_op;

    struct maybe_uint64_t {
        bool present;
        uint64_t value;
    } maybe_uint64_t;

    char * str;
    uint64_t uint;
}

%token T_MATCH T_WHERE T_CREATE T_SET T_REMOVE T_DELETE T_RETURN T_LIMIT T_SKIP
    T_RIGHT_ARROW T_LEFT_ARROW T_IDENTIFIER T_STR_LITERAL T_UINT_LITERAL T_NOT

%left T_OR
%left T_AND

%nonassoc UT_NOT

%type<attribute> attribute
%type<entity> entity
%type<vertex_entity> vertex_entity
%type<edge_entity> edge_entity edge_definition_non_req
%type<request> command
%type<where> where_non_req where where_expr
%type<request_create_op> create_operation
%type<request_set_op> set_operation
%type<request_set_op_change> set_change
%type<request_remove_op> remove_operation
%type<request_remove_op_change> remove_change
%type<request_delete_op> delete_operation
%type<request_return_op> return_operation
%type<request_return_op_value> returnable
%type<array_entity> match_non_req match entity_list
%type<array_str> labels name_list
%type<array_attribute> attributes attribute_list
%type<array_request_set_op_change> set_change_list
%type<array_request_remove_op_change> remove_change_list
%type<array_request_return_op_value> returnable_list
%type<oneof_request_op> operation
%type<maybe_uint64_t> limit_non_req skip_non_req
%type<str> name_non_req name label value label_non_req T_IDENTIFIER T_STR_LITERAL
%type<uint> T_UINT_LITERAL

%%

command_line
    : command semi_non_req YYEOF    { *result = $1; }
    | YYEOF                         { *result = NULL; }
    ;

semi_non_req
    : /* empty */
    | ';'
    ;

command
    : match_non_req where_non_req operation {
        $$ = malloc(sizeof(*$$));
        *$$ = (Request) REQUEST__INIT;

        $$->n_match = $1.amount;
        $$->match = $1.content;

        $$->where = $2;

        $$->op_case = $3._case;
        switch ($3._case) {
        case REQUEST__OP_CREATE:
            $$->create = $3.create;
            break;

        case REQUEST__OP_SET:
            $$->set = $3.set;
            break;

        case REQUEST__OP_REMOVE:
            $$->remove = $3.remove;
            break;

        case REQUEST__OP_DELETE:
            $$->delete_ = $3.delete_;
            break;

        case REQUEST__OP_RETURN:
            $$->return_ = $3.return_;
            break;
        }
    }
    ;

match_non_req
    : /* empty */   { $$.amount = 0; $$.content = NULL; }
    | match
    ;

match
    : T_MATCH entity_list   { $$ = $2; }
    ;

entity_list
    : entity    {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | entity_list ',' entity    {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

entity
    : vertex_entity {
        $$ = malloc(sizeof(*$$));
        *$$ = (Entity) ENTITY__INIT;

        $$->entity_case = ENTITY__ENTITY_VERTEX;
        $$->vertex = $1;
    }
    | edge_entity   {
        $$ = malloc(sizeof(*$$));
        *$$ = (Entity) ENTITY__INIT;

        $$->entity_case = ENTITY__ENTITY_EDGE;
        $$->edge = $1;
    }
    ;

vertex_entity
    : '(' name_non_req labels attributes ')'    {
        $$ = malloc(sizeof(*$$));
        *$$ = (VertexEntity) VERTEX_ENTITY__INIT;

        $$->name = $2;
        $$->n_labels = $3.amount;
        $$->labels = $3.content;
        $$->n_attrs = $4.amount;
        $$->attrs = $4.content;
    }
    ;

name_non_req
    : /* empty */   { $$ = NULL; }
    | name
    ;

name
    : T_IDENTIFIER
    ;

labels
    : /* empty */   { $$.amount = 0; $$.content = NULL; }
    | labels label  {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $2;
        ++$$.amount;
    }
    ;

label
    : ':' name  { $$ = $2; }
    ;

attributes
    : /* empty */               { $$.amount = 0; $$.content = NULL; }
    | '{' attribute_list '}'    { $$ = $2; }
    ;

attribute_list
    : attribute { $$.amount = 1; $$.content = malloc(sizeof(*($$.content))); $$.content[0] = $1; }
    | attribute_list ',' attribute  {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

attribute
    : name ':' value    {
        $$ = malloc(sizeof(*$$));
        *$$ = (Attribute) ATTRIBUTE__INIT;

        $$->name = $1;
        $$->value = $3;
    }
    ;

value
    : T_STR_LITERAL
    ;

edge_entity
    : vertex_entity '-' edge_definition_non_req T_RIGHT_ARROW vertex_entity {
        $$ = $3;
        $$->source = $1;
        $$->destination = $5;
    }
    | vertex_entity T_LEFT_ARROW edge_definition_non_req '-' vertex_entity {
        $$ = $3;
        $$->source = $5;
        $$->destination = $1;
    }
    ;

edge_definition_non_req
    : /* empty */   {
        $$ = malloc(sizeof(*$$));
        *$$ = (EdgeEntity) EDGE_ENTITY__INIT;
    }
    | '[' name_non_req label_non_req ']'    {
        $$ = malloc(sizeof(*$$));
        *$$ = (EdgeEntity) EDGE_ENTITY__INIT;

        $$->name = $2;
        $$->label = $3;
    }
    ;

label_non_req
    : /* empty */   { $$ = NULL; }
    | label
    ;

where_non_req
    : /* empty */   { $$ = NULL; }
    | where
    ;

where
    : T_WHERE where_expr    { $$ = $2; }
    ;

where_expr
    : '(' where_expr ')'    { $$ = $2; }
    | T_NOT where_expr %prec UT_NOT {
        $$ = malloc(sizeof(*$$));
        *$$ = (Where) WHERE__INIT;

        $$->op_case = WHERE__OP_NOT;
        $$->not_ = $2;
    }
    | where_expr T_AND where_expr   {
        $$ = malloc(sizeof(*$$));
        *$$ = (Where) WHERE__INIT;

        $$->op_case = WHERE__OP_AND;
        $$->and_ = malloc(sizeof(*($$->and_)));
        *($$->and_) = (WhereBinary) WHERE_BINARY__INIT;

        $$->and_->left = $1;
        $$->and_->right = $3;
    }
    | where_expr T_OR where_expr    {
        $$ = malloc(sizeof(*$$));
        *$$ = (Where) WHERE__INIT;

        $$->op_case = WHERE__OP_OR;
        $$->or_ = malloc(sizeof(*($$->or_)));
        *($$->or_) = (WhereBinary) WHERE_BINARY__INIT;

        $$->or_->left = $1;
        $$->or_->right = $3;
    }
    | name label    {
        $$ = malloc(sizeof(*$$));
        *$$ = (Where) WHERE__INIT;

        $$->op_case = WHERE__OP_LABEL;
        $$->label = malloc(sizeof(*($$->label)));
        *($$->label) = (WhereLabel) WHERE_LABEL__INIT;

        $$->label->name = $1;
        $$->label->label = $2;
    }
    | name '.' name '=' value   {
        $$ = malloc(sizeof(*$$));
        *$$ = (Where) WHERE__INIT;

        $$->op_case = WHERE__OP_EQUALS;
        $$->equals = malloc(sizeof(*($$->equals)));
        *($$->equals) = (WhereEquals) WHERE_EQUALS__INIT;

        $$->equals->name = $1;
        $$->equals->attr = $3;
        $$->equals->value = $5;
    }
    ;

operation
    : create_operation  { $$._case = REQUEST__OP_CREATE; $$.create = $1; }
    | set_operation     { $$._case = REQUEST__OP_SET; $$.set = $1; }
    | remove_operation  { $$._case = REQUEST__OP_REMOVE; $$.remove = $1; }
    | delete_operation  { $$._case = REQUEST__OP_DELETE; $$.delete_ = $1; }
    | return_operation  { $$._case = REQUEST__OP_RETURN; $$.return_ = $1; }
    ;

create_operation
    : T_CREATE entity_list  {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestCreateOp) REQUEST_CREATE_OP__INIT;

        $$->n_entities = $2.amount;
        $$->entities = $2.content;
    }
    ;

set_operation
    : T_SET set_change_list {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestSetOp) REQUEST_SET_OP__INIT;

        $$->n_changes = $2.amount;
        $$->changes = $2.content;
    }
    ;

set_change_list
    : set_change    {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | set_change_list ',' set_change    {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

set_change
    : name ':' name  {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestSetOpChange) REQUEST_SET_OP_CHANGE__INIT;

        $$->name = $1;
        $$->payload_case = REQUEST_SET_OP_CHANGE__PAYLOAD_LABEL;
        $$->label = $3;
    }
    | name '.' name '=' value {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestSetOpChange) REQUEST_SET_OP_CHANGE__INIT;

        $$->name = $1;
        $$->payload_case = REQUEST_SET_OP_CHANGE__PAYLOAD_ATTR;

        $$->attr = malloc(sizeof(*($$->attr)));
        *($$->attr) = (RequestSetOpChangeAttr) REQUEST_SET_OP_CHANGE_ATTR__INIT;

        $$->attr->attr = $3;
        $$->attr->value = $5;
    }
    ;

remove_operation
    : T_REMOVE remove_change_list   {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestRemoveOp) REQUEST_REMOVE_OP__INIT;

        $$->n_changes = $2.amount;
        $$->changes = $2.content;
    }
    ;

remove_change_list
    : remove_change {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | remove_change_list ',' remove_change {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

remove_change
    : name ':' name  {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestRemoveOpChange) REQUEST_REMOVE_OP_CHANGE__INIT;

        $$->name = $1;
        $$->payload_case = REQUEST_REMOVE_OP_CHANGE__PAYLOAD_LABEL;
        $$->label = $3;
    }
    | name '.' name {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestRemoveOpChange) REQUEST_REMOVE_OP_CHANGE__INIT;

        $$->name = $1;
        $$->payload_case = REQUEST_REMOVE_OP_CHANGE__PAYLOAD_ATTR;
        $$->attr = $3;
    }
    ;

delete_operation
    : T_DELETE name_list    {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestDeleteOp) REQUEST_DELETE_OP__INIT;

        $$->n_names = $2.amount;
        $$->names = $2.content;
    }
    ;

name_list
    : name  { $$.amount = 1; $$.content = malloc(sizeof(*($$.content))); $$.content[0] = $1; }
    | name_list ',' name    {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

return_operation
    : T_RETURN returnable_list limit_non_req skip_non_req   {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestReturnOp) REQUEST_RETURN_OP__INIT;

        $$->n_values = $2.amount;
        $$->values = $2.content;
        $$->has_limit = $3.present;
        $$->limit = $3.value;
        $$->has_skip = $4.present;
        $$->skip = $4.value;
    }
    ;

returnable_list
    : returnable    {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | returnable_list ',' returnable    {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

returnable
    : name  {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestReturnOpValue) REQUEST_RETURN_OP_VALUE__INIT;

        $$->name = $1;
        $$->attr = NULL;
    }
    | name '.' name {
        $$ = malloc(sizeof(*$$));
        *$$ = (RequestReturnOpValue) REQUEST_RETURN_OP_VALUE__INIT;

        $$->name = $1;
        $$->attr = $3;
    }
    ;

limit_non_req
    : /* empty */   { $$.present = false; }
    | T_LIMIT T_UINT_LITERAL    { $$.present = true; $$.value = $2; }
    ;

skip_non_req
    : /* empty */   { $$.present = false; }
    | T_SKIP T_UINT_LITERAL     { $$.present = true; $$.value = $2; }
    ;

%%

void yyerror(Request ** result, char ** error, const char * str) {
    free(*error);

    *error = strdup(str);
}
