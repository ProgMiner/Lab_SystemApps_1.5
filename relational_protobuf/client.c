#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdbool.h>
#include <inttypes.h>

#include "api.pb-c.h"
#include "utils.h"
#include "y.tab.h"


void scan_string(const char * str);

static bool is_error_response(const Response * response) {
    if (response->payload_case == RESPONSE__PAYLOAD_ERROR) {
        printf("Error: %s.\n", response->error);
        return true;
    }

    return false;
}

static const SuccessResponse * get_success_response(const Response * response) {
    if (response->payload_case == RESPONSE__PAYLOAD_SUCCESS) {
        return response->success;
    }

    printf("Bad answer.\n");
    return NULL;
}

static char * render_Value(const Value * value) {
    char * buf;

    switch (value->value_case) {
        case VALUE__VALUE__NOT_SET:
            return strdup("NULL");

        case VALUE__VALUE_INT:
            buf = malloc(sizeof(char) * 128);
            snprintf(buf, 127, "%"PRIi64, value->int_);
            buf[127] = '\0';
            return buf;

        case VALUE__VALUE_UINT:
            buf = malloc(sizeof(char) * 128);
            snprintf(buf, 127, "%"PRIu64, value->uint);
            buf[127] = '\0';
            return buf;

        case VALUE__VALUE_NUM:
            buf = malloc(sizeof(char) * 128);
            snprintf(buf, 127, "%lf", value->num);
            buf[127] = '\0';
            return buf;

        case VALUE__VALUE_STR:
            return strdup(value->str);

        default:
            return NULL;
    }
}

static void print_amount_response(const SuccessResponse * response, const char * action) {
    if (response->value_case != SUCCESS_RESPONSE__VALUE_AMOUNT) {
        printf("Bad answer.\n");
        return;
    }

    printf("%lu rows was %s.\n", response->amount, action);
}

static void print_table_separator(unsigned int columns_length, const unsigned int * columns_width) {
    for (size_t i = 0; i < columns_length; ++i) {
        putchar('+');

        for (size_t j = 0; j < columns_width[i] + 2; ++j) {
            putchar('-');
        }
    }

    puts("+");
}

static void print_table_response(const SuccessResponse * response) {
    if (response->value_case != SUCCESS_RESPONSE__VALUE_TABLE) {
        printf("Bad answer.\n");
        return;
    }

    const Table * table = response->table;

    char ** const cell_strings = calloc(table->n_columns * table->n_rows, sizeof(char *));
    unsigned int columns_width[table->n_columns];

    for (size_t i = 0; i < table->n_columns; ++i) {
        columns_width[i] = (unsigned int) strlen(table->columns[i]);
    }

    for (size_t i = 0; i < table->n_rows; ++i) {
        const Table__Row * const row = table->rows[i];

        for (size_t j = 0; j < table->n_columns; ++j) {
            const char * const str = cell_strings[i * table->n_columns + j] = render_Value(row->cells[j]);

            const unsigned int width = (unsigned int) strlen(str);
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
            free(cell_strings[i * table->n_columns + j]);
        }

        puts("|");
    }

    print_table_separator(table->n_columns, columns_width);
    free(cell_strings);
}

static void print_response(Request__ActionCase action, const Response * response) {
    if (is_error_response(response)) {
        return;
    }

    const SuccessResponse * success_response = get_success_response(response);
    if (!success_response) {
        return;
    }

    switch (action) {
        case REQUEST__ACTION_CREATE_TABLE:
            printf("Table was created.\n");
            break;

        case REQUEST__ACTION_DROP_TABLE:
            printf("Table was dropped.\n");
            break;

        case REQUEST__ACTION_INSERT:
            printf("Row was inserted.\n");
            break;

        case REQUEST__ACTION_DELETE:
            print_amount_response(success_response, "deleted");
            break;

        case REQUEST__ACTION_SELECT:
            print_table_response(success_response);
            break;

        case REQUEST__ACTION_UPDATE:
            print_amount_response(success_response, "updated");
            break;

        default:
            return;
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
    if (!write_full(socket, &request_size_n, sizeof(request_size_n))) {
        free(request_buffer);
        return false;
    }

    if (!write_full(socket, request_buffer, request_size)) {
        free(request_buffer);
        return false;
    }

    free(request_buffer);

    uint32_t response_size;
    if (!read_full(socket, &response_size, sizeof(response_size))) {
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
        printf("Server didn't understand request.\n");
        return true;
    }

    print_response(request->action_case, response);
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
    }

    // and then close the socket
    close(client_socket);

    printf("Bye!\n");
    return 0;
}
