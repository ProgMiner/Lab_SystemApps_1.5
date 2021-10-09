cmake_minimum_required(VERSION 3.18)
project(lab1_5_spo C)

set(CMAKE_C_STANDARD 11)

add_compile_options(-fsanitize=address)
add_link_options(-fsanitize=address)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH};${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Flex  REQUIRED)
find_package(Bison REQUIRED)
find_package(ProtobufC REQUIRED)
protoc(API_SRC api.proto)

add_executable(server server.c storage.c storage.h utils.c utils.h ${API_SRC})
target_include_directories(server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(server ${PROTOBUFC_LIBRARIES})

add_executable(client client.c utils.c utils.h ${API_SRC} ${CMAKE_CURRENT_BINARY_DIR}/lex.yy.c
    ${CMAKE_CURRENT_BINARY_DIR}/y.tab.c ${CMAKE_CURRENT_BINARY_DIR}/y.tab.h)
target_include_directories(client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(client ${PROTOBUFC_LIBRARIES})

add_custom_command(
        MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/ql.lex"
        PRE_BUILD
        COMMAND ${FLEX_EXECUTABLE}
        ARGS -o"${CMAKE_CURRENT_BINARY_DIR}/lex.yy.c" "${CMAKE_CURRENT_SOURCE_DIR}/ql.lex"
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/lex.yy.c"
)

add_custom_command(
        MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/ql.y"
        PRE_BUILD
        COMMAND ${BISON_EXECUTABLE}
        ARGS -y -d --debug --verbose -o"${CMAKE_CURRENT_BINARY_DIR}/y.tab.c" -Wcounterexamples
        "${CMAKE_CURRENT_SOURCE_DIR}/ql.y"
        DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/lex.yy.c"
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/y.tab.c"
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/y.tab.h"
)
