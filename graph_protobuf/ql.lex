%option noyywrap case-insensitive

%{
#include "api.pb-c.h"
#include "y.tab.h"


static char * identifier_str() {
    char * const str = malloc(sizeof(char) * (yyleng + 1));

    if (str) {
        memcpy(str, yytext, yyleng);
        str[yyleng] = '\0';
    }

    return str;
}

static char * quoted_str() {
    char * str = malloc(sizeof(*str) * yyleng);

    int j = 0;
    for (int i = 1; i < yyleng - 1; ++i, ++j) {
        if (yytext[i] == '\\') {
            ++i;
        }

        str[j] = yytext[i];
    }

    str[j] = '\0';
    return str;
}

static uint64_t uint_literal() {
    char * str = malloc(sizeof(*str) * (yyleng + 1));

    memcpy(str, yytext, yyleng);
    str[yyleng] = '\0';

    uint64_t val;
    sscanf(str, "%"SCNu64, &val);
    free(str);

    return val;
}
%}

S [ \n\b\t\f\r]
W [a-zA-Z_]
D [0-9]

I {W}({W}|{D})*

%%

{S}     ;

match   return T_MATCH;
where   return T_WHERE;
create  return T_CREATE;
set     return T_SET;
remove  return T_REMOVE;
delete  return T_DELETE;
return  return T_RETURN;
limit   return T_LIMIT;
skip    return T_SKIP;
"->"    return T_RIGHT_ARROW;
"<-"    return T_LEFT_ARROW;

and     return T_AND;
"&&"    return T_AND;
or      return T_OR;
"||"    return T_OR;

{I}     yylval.str = identifier_str(); return T_IDENTIFIER;

{D}+                yylval.uint = uint_literal(); return T_UINT_LITERAL;
\'(\\.|[^'\\])*\'   yylval.str = quoted_str(); return T_STR_LITERAL;
\"(\\.|[^"\\])*\"   yylval.str = quoted_str(); return T_STR_LITERAL;

.       return yytext[0];

%%

void scan_string(const char * str) {
    yy_switch_to_buffer(yy_scan_string(str));
}
