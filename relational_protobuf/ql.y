%parse-param {Request ** result} {char ** error}

%code requires {
#include <stdbool.h>
}

%{
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include "api.pb-c.h"


static Request * make_request(Request__ActionCase action_case, void * action);

int yylex(void);
void yyerror(Request ** result, char ** error, const char * str);
%}

%union {
    ValueType value_type;
    Value * value;
    Request * request;
    CreateTableRequest * create_table_request;
    CreateTableRequest__Column * create_table_request__column;
    DropTableRequest * drop_table_request;
    InsertRequest * insert_request;
    DeleteRequest * delete_request;
    SelectRequest * select_request;
    SelectRequest__Join * select_request__join;
    UpdateRequest * update_request;
    WhereExpr * where_expr;

    struct ql_update_request_set {
        char * column;
        Value * value;
    } update_request_set;

    struct {
        size_t amount;
        CreateTableRequest__Column ** content;
    } array_CreateTableRequest__Column;

    struct {
        size_t amount;
        Value ** content;
    } array_Value;

    struct {
        size_t amount;
        SelectRequest__Join ** content;
    } array_SelectRequest__Join;

    struct {
        size_t amount;
        struct ql_update_request_set * content;
    } array_ql_update_request_set;

    struct {
        size_t amount;
        char ** content;
    } array_str;

    struct {
        bool present;
        uint64_t value;
    } maybe_uint64;

    char * str;
    int64_t int64;
    uint64_t uint64;
    double double_;
}

%token T_CREATE T_TABLE T_INT T_UINT T_NUM T_STR T_DROP T_INSERT T_VALUES T_INTO
    T_NULL T_DELETE T_FROM T_WHERE T_JOIN T_ON T_EQ_OP T_NE_OP T_LT_OP T_GT_OP T_LE_OP T_GE_OP
    T_SELECT T_ASTERISK T_OFFSET T_LIMIT T_UPDATE T_SET

%token<str> T_IDENTIFIER T_DBL_QUOTED T_STR_LITERAL
%token<int64> T_INT_LITERAL
%token<uint64> T_UINT_LITERAL
%token<double_> T_NUM_LITERAL

%left T_OR_OP
%left T_AND_OP

%type<value_type> type
%type<value> value
%type<request> command
%type<create_table_request> create_table_command
%type<create_table_request__column> column_declaration
%type<drop_table_request> drop_table_command
%type<insert_request> insert_command
%type<delete_request> delete_command
%type<select_request> select_command
%type<select_request__join> join_stmt
%type<update_request> update_command
%type<where_expr> where_stmt_non_req where_stmt where_expr
%type<update_request_set> update_value
%type<array_CreateTableRequest__Column> columns_declaration_list columns_declaration_list_req
%type<array_Value> values_list values_list_req
%type<array_SelectRequest__Join> join_stmts join_stmts_non_null
%type<array_ql_update_request_set> update_values_list_req
%type<array_str> braced_names_list_non_req braced_names_list names_list_req names_list_or_asterisk
%type<maybe_uint64> offset_stmt_non_req limit_stmt_non_req
%type<str> name
%type<uint64> offset_stmt limit_stmt

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
    : create_table_command  { $$ = make_request(REQUEST__ACTION_CREATE_TABLE, $1); }
    | drop_table_command    { $$ = make_request(REQUEST__ACTION_DROP_TABLE, $1); }
    | insert_command        { $$ = make_request(REQUEST__ACTION_INSERT, $1); }
    | delete_command        { $$ = make_request(REQUEST__ACTION_DELETE, $1); }
    | select_command        { $$ = make_request(REQUEST__ACTION_SELECT, $1); }
    | update_command        { $$ = make_request(REQUEST__ACTION_UPDATE, $1); }
    ;

create_table_command
    : T_CREATE t_table_non_req name '(' columns_declaration_list ')'    {
        $$ = malloc(sizeof(CreateTableRequest));
        create_table_request__init($$);

        $$->table = $3;
        $$->n_columns = $5.amount;
        $$->columns = $5.content;
    }
    ;

t_table_non_req
    : /* empty */
    | T_TABLE
    ;

name
    : T_IDENTIFIER  { $$ = $1; }
    | T_DBL_QUOTED  { $$ = $1; }
    ;

columns_declaration_list
    : /* empty */                   { $$.amount = 0; $$.content = NULL; }
    | columns_declaration_list_req  { $$ = $1; }
    ;

columns_declaration_list_req
    : column_declaration    {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | columns_declaration_list_req ',' column_declaration   {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

column_declaration
    : name type     {
        $$ = malloc(sizeof(CreateTableRequest__Column));
        create_table_request__column__init($$);

        $$->name = $1;
        $$->type = $2;
    }
    ;

type
    : T_INT     { $$ = VALUE_TYPE__INT; }
    | T_UINT    { $$ = VALUE_TYPE__UINT; }
    | T_NUM     { $$ = VALUE_TYPE__NUM; }
    | T_STR     { $$ = VALUE_TYPE__STR; }
    ;

drop_table_command
    : T_DROP t_table_non_req name   {
        $$ = malloc(sizeof(DropTableRequest));
        drop_table_request__init($$);

        $$->table = $3;
    }
    ;

insert_command
    : T_INSERT t_into_non_req name braced_names_list_non_req T_VALUES '(' values_list ')'   {
        $$ = malloc(sizeof(InsertRequest));
        insert_request__init($$);

        $$->table = $3;
        $$->n_columns = $4.amount;
        $$->columns = $4.content;
        $$->n_values = $7.amount;
        $$->values = $7.content;
    }
    ;

t_into_non_req
    : /* empty */
    | T_INTO
    ;

braced_names_list_non_req
    : /* empty */           { $$.amount = 0; $$.content = NULL; }
    | braced_names_list     { $$ = $1; }
    ;

braced_names_list
    : '(' names_list_req ')'    { $$ = $2; }
    ;

names_list_req
    : name  {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | names_list_req ',' name   {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

values_list
    : /* empty */       { $$.amount = 0; $$.content = NULL; }
    | values_list_req   { $$ = $1; }
    ;

values_list_req
    : value {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | values_list_req ',' value {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

value
    : T_INT_LITERAL     {
        $$ = malloc(sizeof(Value));
        value__init($$);

        $$->value_case = VALUE__VALUE_INT;
        $$->int_ = $1;
    }
    | T_UINT_LITERAL    {
        $$ = malloc(sizeof(Value));
        value__init($$);

        $$->value_case = VALUE__VALUE_UINT;
        $$->uint = $1;
    }
    | T_NUM_LITERAL     {
        $$ = malloc(sizeof(Value));
        value__init($$);

        $$->value_case = VALUE__VALUE_NUM;
        $$->num = $1;
    }
    | T_STR_LITERAL     {
        $$ = malloc(sizeof(Value));
        value__init($$);

        $$->value_case = VALUE__VALUE_STR;
        $$->str = $1;
    }
    | T_NULL    {
        $$ = malloc(sizeof(Value));
        value__init($$);

        $$->value_case = VALUE__VALUE__NOT_SET;
    }
    ;

delete_command
    : T_DELETE T_FROM name where_stmt_non_req   {
        $$ = malloc(sizeof(DeleteRequest));
        delete_request__init($$);

        $$->table = $3;
        $$->where = $4;
    }
    ;

where_stmt_non_req
    : /* empty */   { $$ = NULL; }
    | where_stmt    { $$ = $1; }
    ;

where_stmt
    : T_WHERE where_expr    { $$ = $2; }
    ;

where_expr
    : '(' where_expr ')'    { $$ = $2; }
    | name T_EQ_OP value    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_EQ;
        $$->eq = malloc(sizeof(WhereValueOp));
        where_value_op__init($$->eq);

        $$->eq->column = $1;
        $$->eq->value = $3;
    }
    | name T_NE_OP value    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_NE;
        $$->ne = malloc(sizeof(WhereValueOp));
        where_value_op__init($$->ne);

        $$->ne->column = $1;
        $$->ne->value = $3;
    }
    | name T_LT_OP value    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_LT;
        $$->lt = malloc(sizeof(WhereValueOp));
        where_value_op__init($$->lt);

        $$->lt->column = $1;
        $$->lt->value = $3;
    }
    | name T_GT_OP value    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_GT;
        $$->gt = malloc(sizeof(WhereValueOp));
        where_value_op__init($$->gt);

        $$->gt->column = $1;
        $$->gt->value = $3;
    }
    | name T_LE_OP value    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_LE;
        $$->le = malloc(sizeof(WhereValueOp));
        where_value_op__init($$->le);

        $$->le->column = $1;
        $$->le->value = $3;
    }
    | name T_GE_OP value    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_GE;
        $$->ge = malloc(sizeof(WhereValueOp));
        where_value_op__init($$->ge);

        $$->ge->column = $1;
        $$->ge->value = $3;
    }
    | where_expr T_AND_OP where_expr    {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_AND;
        $$->and_ = malloc(sizeof(WhereExprOp));
        where_expr_op__init($$->and_);

        $$->and_->left = $1;
        $$->and_->right = $3;
    }
    | where_expr T_OR_OP where_expr {
        $$ = malloc(sizeof(WhereExpr));
        where_expr__init($$);

        $$->op_case = WHERE_EXPR__OP_OR;
        $$->or_ = malloc(sizeof(WhereExprOp));
        where_expr_op__init($$->or_);

        $$->or_->left = $1;
        $$->or_->right = $3;
    }
    ;

select_command
    : T_SELECT names_list_or_asterisk T_FROM name join_stmts where_stmt_non_req offset_stmt_non_req limit_stmt_non_req {
        $$ = malloc(sizeof(SelectRequest));
        select_request__init($$);

        $$->table = $4;
        $$->n_columns = $2.amount;
        $$->columns = $2.content;
        $$->where = $6;
        $$->has_offset = $7.present;
        $$->offset = $7.value;
        $$->has_limit = $8.present;
        $$->limit = $8.value;
        $$->n_joins = $5.amount;
        $$->joins = $5.content;
    }
    ;

names_list_or_asterisk
    : names_list_req    { $$ = $1; }
    | T_ASTERISK        { $$.amount = 0; $$.content = NULL; }
    ;

join_stmts
    : /* empty */           { $$.amount = 0; $$.content = NULL; }
    | join_stmts_non_null   { $$ = $1; }
    ;

join_stmts_non_null
    : join_stmt {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | join_stmts_non_null join_stmt  {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $2;
        ++$$.amount;
    }
    ;

join_stmt
    : T_JOIN name T_ON name T_EQ_OP name    {
        $$ = malloc(sizeof(SelectRequest__Join));
        select_request__join__init($$);

        $$->table = $2;
        $$->t_column = $4;
        $$->s_column = $6;
    }
    ;

offset_stmt_non_req
    : /* empty */   { $$.present = false; }
    | offset_stmt   { $$.present = true; $$.value = $1; }
    ;

offset_stmt
    : T_OFFSET T_UINT_LITERAL   { $$ = $2; }
    ;

limit_stmt_non_req
    : /* empty */   { $$.present = false; }
    | limit_stmt    { $$.present = true; $$.value = $1; }
    ;

limit_stmt
    : T_LIMIT T_UINT_LITERAL    { $$ = $2; }
    ;

update_command
    : T_UPDATE name T_SET update_values_list_req where_stmt_non_req {
        $$ = malloc(sizeof(UpdateRequest));
        update_request__init($$);

        $$->table = $2;

        const size_t c = $4.amount;
        $$->n_columns = c;
        $$->columns = malloc(sizeof(char *) * c);
        $$->n_values = c;
        $$->values = malloc(sizeof(Value *) * c);

        for (size_t i = 0; i < c; ++i) {
            $$->columns[i] = $4.content[i].column;
            $$->values[i] = $4.content[i].value;
        }

        $$->where = $5;
    }
    ;

update_values_list_req
    : update_value  {
        $$.amount = 1;
        $$.content = malloc(sizeof(*($$.content)));
        $$.content[0] = $1;
    }
    | update_values_list_req ',' update_value   {
        $$ = $1;
        $$.content = realloc($$.content, sizeof(*($$.content)) * ($$.amount + 1));
        $$.content[$$.amount] = $3;
        ++$$.amount;
    }
    ;

update_value
    : name T_EQ_OP value    { $$.column = $1; $$.value = $3; }
    ;

%%

static Request * make_request(Request__ActionCase action_case, void * action) {
    Request * const result = malloc(sizeof(Request));
    request__init(result);

    result->action_case = action_case;

    switch (action_case) {
        case REQUEST__ACTION_CREATE_TABLE:
        result->create_table = action;
        break;

        case REQUEST__ACTION_DROP_TABLE:
        result->drop_table = action;
        break;

        case REQUEST__ACTION_INSERT:
        result->insert = action;
        break;

        case REQUEST__ACTION_DELETE:
        result->delete_ = action;
        break;

        case REQUEST__ACTION_SELECT:
        result->select = action;
        break;

        case REQUEST__ACTION_UPDATE:
        result->update = action;
        break;

        default:
        break;
    }

    return result;
}

void yyerror(Request ** result, char ** error, const char * str) {
    free(*error);

    *error = strdup(str);
}
