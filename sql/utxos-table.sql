CREATE TABLE utxos  (
    utxo_id text PRIMARY KEY, 
    tx_hash text, 
    tx_index integer, 
    receiver text, 
    amount bigint,
    block_num integer
);

-- Indexes
CREATE INDEX ON utxos (receiver);
