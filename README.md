# Mini RDBMS (Educational Project)

This project is a minimal relational database management system implemented in Python.
It is designed for learning and demonstration purposes, not for production use.

The goal of this project is to demonstrate clear thinking about database internals:
schemas, persistence, query parsing, execution, constraints, indexing, and joins.

---

## Scope and Philosophy

This system prioritizes:
- clarity over performance
- correctness over completeness
- explicit limitations over hidden complexity

It intentionally avoids advanced database features in order to remain understandable.

---

## What This System Is

- A single-user, in-process relational database engine
- A SQL-inspired interface with a limited grammar
- A learning-focused implementation with explicit trade-offs
- A backend for a trivial web application that demonstrates CRUD

---

## What This System Is Not

This system is NOT:
- ANSI SQL compliant
- transactional (no ACID guarantees)
- concurrent or multi-user
- optimized for large datasets
- secure against malicious input
- suitable for production use

---

## Supported Data Types

- INT   → Python int
- TEXT  → Python str
- BOOL  → Python bool

NULL values are not supported.

---

## Supported SQL Subset

### Table Definition
- CREATE TABLE table_name (...)

Features:
- column definitions with types
- exactly one PRIMARY KEY
- zero or more UNIQUE columns

---

### Data Mutation
- INSERT INTO table_name VALUES (...)
- UPDATE table_name SET column=value WHERE column=value
- DELETE FROM table_name WHERE column=value

Rules:
- WHERE supports only a single equality condition
- No AND / OR
- No expressions

---

### Data Retrieval
- SELECT column_list FROM table_name WHERE column=value
- SELECT * FROM table_name

Optional:
- SELECT ... FROM table1 INNER JOIN table2 ON column=column

Rules:
- INNER JOIN only
- equality join only
- one join per query
- no subqueries
- no aggregation

---

## Constraints

The engine enforces:
- PRIMARY KEY uniqueness
- UNIQUE column uniqueness
- type correctness on INSERT and UPDATE

Constraint violations raise clear, human-readable errors.
Partial writes are not allowed.

---

## Storage Model

- Schemas stored in a catalog file (catalog.json)
- Each table stored in its own file
- Rows represented as dictionaries
- Data loaded into memory at startup
- Data flushed to disk on mutation

---

## Indexing

- In-memory hash-map–based indexes
- Used for primary keys, unique constraints, and equality filters
- Rebuilt on startup
- Updated on data mutation

---

## Interface

- Interactive REPL for executing queries
- Clear error messages
- Non-crashing behavior on invalid input

---

## Web Application

A simple Flask-based web application demonstrates CRUD operations using this database engine.
The application interacts exclusively with the custom DB engine.

---

## Limitations and Future Work

This system is intentionally limited.
Possible future improvements include:
- transactions
- concurrency
- richer query grammar
- better indexing strategies
- durability guarantees

These are explicitly out of scope for this project.
