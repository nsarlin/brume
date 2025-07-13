-- This file should undo anything in `up.sql`
ALTER TABLE synchros RENAME TO synchros_old;
ALTER TABLE filesystems RENAME TO filesystems_old;
ALTER TABLE nodes RENAME TO nodes_old;

CREATE TABLE nodes (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  name VARCHAR NOT NULL,
  kind VARCHAR NOT NULL,
  state BLOB,
  size BIGINT,
  parent INTEGER REFERENCES nodes(id),
  last_modified DATETIME
);

CREATE TABLE filesystems (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  uuid BLOB NOT NULL UNIQUE,
  creation_info BLOB NOT NULL,
  root_node INTEGER NOT NULL REFERENCES nodes(id)
);

CREATE TABLE synchros (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  uuid BLOB NOT NULL UNIQUE,
  name VARCHAR NOT NULL,
  local_fs INTEGER NOT NULL REFERENCES filesystems(id),
  remote_fs INTEGER NOT NULL REFERENCES filesystems(id),
  status VARCHAR NOT NULL,
  state VARCHAR NOT NULL
);

INSERT INTO nodes (id, name, kind, state, size, parent, last_modified)
SELECT id, name, kind, state, size, parent, last_modified
FROM nodes_old;

INSERT INTO filesystems (id, uuid, creation_info, root_node)
SELECT id, uuid, creation_info, root_node
FROM filesystems_old;

INSERT INTO synchros (id, uuid, name, local_fs, remote_fs, status, state)
SELECT id, uuid, name, local_fs, remote_fs, status, state
FROM synchros_old;

DROP TABLE synchros_old;
DROP TABLE filesystems_old;
DROP TABLE nodes_old;
ALTER TABLE nodes RENAME TO nodes_old;

CREATE TABLE nodes (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  name VARCHAR NOT NULL,
  kind VARCHAR NOT NULL,
  state BLOB,
  size BIGINT,
  -- The foreign key is now back to the original, without the cascade rule
  parent INTEGER REFERENCES nodes(id),
	last_modified DATETIME
);

INSERT INTO nodes (id, name, kind, state, size, parent)
SELECT id, name, kind, state, size, parent
FROM nodes_old;

DROP TABLE nodes_old;
