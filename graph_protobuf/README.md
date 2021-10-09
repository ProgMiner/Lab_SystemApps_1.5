# lab 1.5 spo

## File structure

```
File structure:
- storage_header - storage header
- heap

storage_pointer<T>:
- uint64_t - offset from the start of file in bytes to the value of type T

storage_list<T>:
- storage_pointer<storage_list_node<T>> - pointer to the first node
- storage_pointer<storage_list_node<T>> - pointer to the last node

storage_list_node<T>:
- storage_pointer<storage_list_node<T>> - pointer to the next node
- storage_pointer<T> - pointer to the value of type T

storage_string:
- uint64_t - length
- uint8_t[length] - value

storage_attribute:
- storage_string - name
- storage_pointer<storage_string> - pointer to the value

storage_vertex:
- storage_list<storage_string> - list of labels
- storage_list<storage_attribute> - list of attributes

storage_edge:
- storage_pointer<storage_string> - label
- storage_pointer<storage_list_node<storage_vertex>> - pointer to the list node with source vertex
- storage_pointer<storage_list_node<storage_vertex>> - pointer to the list node with destination vertex

storage_header:
- uint8_t[4] - signature
- storage_list<storage_vertex> - list of vertices
- storage_list<storage_edge> - list of edges
```
