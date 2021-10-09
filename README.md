# Lab_SystemApps_1.5

A collection of 1.5 laboratory works that I wrote.

The task of this laboratory work is to write a database of specified type (relational/graph/document)
in C language, using specified protocol/language (JSON/protocol buffers/XML/Apache Thrift)
for communication between client and server sides, and implement a subset of any existing
query language used for that type of database (e.g. SQL for relational, Cypher for graph, etc.)

Full task text in russian is [here](spo-lab-1.5.pdf).

- [relational_json](relational_json)
  - [relational database](https://en.wikipedia.org/wiki/Relational_database)
  - JSON between client and server
  - SQL-like language as a query language

- [relational_protobuf](relational_protobuf)
  - [relational database](https://en.wikipedia.org/wiki/Relational_database)
  - protocol buffers between client and server
  - SQL-like language as a query language

- [graph_protobuf](graph_protobuf)
  - [graph database](https://en.wikipedia.org/wiki/Graph_database)
  - protocol buffers between client and server
  - Cypher-like language as a query language
