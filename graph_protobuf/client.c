#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdbool.h>
#include <errno.h>

#include "api.pb-c.h"
#include "y.tab.h"
#include "utils.h"


void scan_string(const char * str);

static bool is_error_response(const Response * response) {
    if (response->payload_case == RESPONSE__PAYLOAD_ERROR) {
        printf("Error: %s.\n", response->error);
        return true;
    }

    return false;
}

static const ResponseSuccess * get_success_response(const Response * response) {
    if (response->payload_case == RESPONSE__PAYLOAD_SUCCESS) {
        return response->success;
    }

    printf("Bad answer.\n");
    return NULL;
}

static void print_amount_response(const ResponseSuccess * response, const char * what, const char * action) {
    if (response->value_case != RESPONSE_SUCCESS__VALUE_AMOUNT) {
        printf("Bad answer.\n");
        return;
    }

    printf("%lu %s was %s.\n", response->amount, what, action);
}

static void print_table_separator(size_t columns_length, const unsigned int * columns_width) {
    for (size_t i = 0; i < columns_length; ++i) {
        putchar('+');

        for (unsigned int j = 0; j < columns_width[i] + 2; ++j) {
            putchar('-');
        }
    }

    puts("+");
}

static char * render_vertex(const VertexEntity * vertex) {
    if (!vertex) {
        return strdup("NULL");
    }

    char * result = malloc(sizeof(char) * 65536);
    size_t pos = 0;

    if (vertex->name) {
        pos += sprintf(result + pos, "%s", vertex->name);
    }

    for (size_t i = 0; i < vertex->n_labels; ++i) {
        pos += sprintf(result + pos, ":%s", vertex->labels[i]);
    }

    if (pos > 0) {
        pos += sprintf(result + pos, " ");
    }

    if (vertex->n_attrs > 0) {
        pos += sprintf(result + pos, "{ ");
    }

    for (size_t i = 0; i < vertex->n_attrs; ++i) {
        if (i > 0) {
            pos += sprintf(result + pos, ", ");
        }

        pos += sprintf(result + pos, "%s: \"%s\"", vertex->attrs[i]->name, vertex->attrs[i]->value);
    }

    if (vertex->n_attrs > 0) {
        pos += sprintf(result + pos, " }");
    }

    return realloc(result, pos);
}

static char * render_value(const ResponseSuccessCell * cell) {
    switch (cell->value_case) {
        case RESPONSE_SUCCESS_CELL__VALUE__NOT_SET:
            return strdup("NULL");

        case RESPONSE_SUCCESS_CELL__VALUE_STRING:
            return strdup(cell->string);

        case RESPONSE_SUCCESS_CELL__VALUE_VERTEX:
            return render_vertex(cell->vertex);

        default:
            return NULL;
    }
}

static void print_table_response(const ResponseSuccess * response) {
    if (response->value_case != RESPONSE_SUCCESS__VALUE_TABLE) {
        printf("Bad answer.\n");
        return;
    }

    const ResponseSuccessTable * table = response->table;

    char ** const cell_strings = calloc(table->n_columns * table->n_rows, sizeof(char *));
    unsigned int * const columns_width = calloc(table->n_columns, sizeof(unsigned int));

    for (size_t i = 0; i < table->n_columns; ++i) {
        columns_width[i] = (unsigned int) strlen_utf8(table->columns[i]);
    }

    for (size_t i = 0; i < table->n_rows; ++i) {
        const ResponseSuccessRow * row = table->rows[i];

        for (size_t j = 0; j < table->n_columns; ++j) {
            const char * const str = cell_strings[i * table->n_columns + j] = render_value(row->cells[j]);

            const unsigned int width = strlen_utf8(str);
            columns_width[j] = columns_width[j] > width ? columns_width[j] : width;
        }
    }

    print_table_separator(table->n_columns, columns_width);

    for (size_t i = 0; i < table->n_columns; ++i) {
        printf("| %*s ", columns_width[i], table->columns[i]);
    }

    puts("|");

    for (size_t i = 0; i < table->n_rows; ++i) {
        print_table_separator(table->n_columns, columns_width);

        for (size_t j = 0; j < table->n_columns; ++j) {
            printf("| %*s ", columns_width[j], cell_strings[i * table->n_columns + j]);
        }

        puts("|");
    }

    print_table_separator(table->n_columns, columns_width);
}

static void print_response(Request__OpCase op, const Response * response) {
    if (!response) {
        printf("Server didn't understand request.\n");
        return;
    }

    if (is_error_response(response)) {
        return;
    }

    const ResponseSuccess * success_response = get_success_response(response);
    if (!success_response) {
        return;
    }

    switch (op) {
        case REQUEST__OP_CREATE:
            print_amount_response(success_response, "entities", "created");
            break;

        case REQUEST__OP_SET:
            print_amount_response(success_response, "properties", "updated");
            break;

        case REQUEST__OP_REMOVE:
            print_amount_response(success_response, "properties", "removed");
            break;

        case REQUEST__OP_DELETE:
            print_amount_response(success_response, "entities", "deleted");
            break;

        case REQUEST__OP_RETURN:
            print_table_response(success_response);
            break;

        default:
            break;
    }
}

static bool handle_request(int socket, const Request * request) {
    size_t request_size = request__get_packed_size(request);

    if (request_size > UINT32_MAX) {
        printf("Too complex request.\n");
        return true;
    }

    uint8_t * const request_buffer = malloc(request_size);
    if (!request_buffer) {
        return false;
    }

    request_size = request__pack(request, request_buffer);

    const uint32_t request_size_n = htonl((uint32_t) request_size);
    if (!write_full_value(socket, &request_size_n)) {
        free(request_buffer);
        return false;
    }

    if (!write_full(socket, request_buffer, request_size)) {
        free(request_buffer);
        return false;
    }

    free(request_buffer);

    uint32_t response_size;
    if (!read_full_value(socket, &response_size)) {
        return false;
    }

    response_size = ntohl(response_size);
    if (response_size == 0) {
        printf("Empty answer.\n");
        return true;
    }

    uint8_t * const response_buffer = malloc(response_size);
    if (!read_full(socket, response_buffer, response_size)) {
        return false;
    }

    Response * const response = response__unpack(NULL, response_size, response_buffer);
    if (!response) {
        printf("Bad answer.\n");
        return true;
    }

    print_response(request->op_case, response);
    return true;
}

static bool handle_command(int socket, const char * command) {
    char * error = NULL;
    Request * request;

    scan_string(command);
    if (yyparse(&request, &error) != 0) {
        printf("Parsing error: %s.\n", error);
        return true;
    }

    if (!request) {
        return true;
    }

    return handle_request(socket, request);
}

int main() {
    // create a socket
    int client_socket = socket(AF_INET, SOCK_STREAM, 0);

    // specify an address for the socket
    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(9002);
    server_address.sin_addr.s_addr = INADDR_ANY;

    // check for error with the connection
    if (connect(client_socket, (struct sockaddr *) &server_address, sizeof(server_address)) != 0) {
        perror("There was an error making a connection to the remote socket");
        return -1;
    }

    bool working = true;
    while (working) {
        size_t command_capacity = 0;
        char * command = NULL;

        printf("> ");
        fflush(stdout);

        ssize_t was_read = getline(&command, &command_capacity, stdin);
        if (was_read <= 0) {
            free(command);
            break;
        }

        command[was_read] = '\0';
        working = handle_command(client_socket, command);

        if (!working && errno) {
            perror("Error");
        }
    }

    // and then close the socket
    close(client_socket);

    printf("Bye!\n");
    return 0;
}
