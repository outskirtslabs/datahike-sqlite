# Datahike SQLite Storage

This context names the persistence concepts that connect Datahike, Konserve,
and SQLite. It distinguishes a logical Datahike database from its Konserve store
and physical SQLite database file.

## Language

### Persistence layers

**Datahike database**:
The logical Datalog database that applications transact against and query. Its durable state lives in a Konserve store.
_Avoid_: SQLite database, store

**SQLite backend**:
The adapter that persists a Datahike database's Konserve store in SQLite.
_Avoid_: Driver, database

**Konserve store**:
The logical key-value namespace between Datahike and SQLite. It contains store entries and has a stable store identity.
_Avoid_: Datahike database, SQLite database file, backing store

**SQLite database file**:
The physical SQLite container that can hold one or more Konserve stores.
_Avoid_: Datahike database, Konserve store

**Store table**:
The SQLite table that contains one Konserve store's entries. One SQLite database file may contain multiple store tables.
_Avoid_: Datahike database, SQLite database file

### Store contents and identity

**Store entry**:
A key-addressed value held in a Konserve store.
_Avoid_: Datahike entity, SQLite row, blob

**Store key**:
The identifier under which Konserve addresses a store entry.
_Avoid_: Entity ID, row ID

**Store ID**:
The stable UUID that identifies a Konserve store across connections.
_Avoid_: Entity ID, row ID

**Store configuration**:
The description that selects a Konserve store by its SQLite database file, store table, and store identity.
_Avoid_: Datahike database, connection
