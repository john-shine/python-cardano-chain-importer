CREATE TABLE blocks  (
  block_hash TEXT PRIMARY KEY,
  epoch integer,
  slot integer
);

ALTER TABLE blocks ADD COLUMN block_height integer;