CREATE TABLE txs (
    hash TEXT  PRIMARY KEY, 
    inputs json,
    inputs_address TEXT, 
    inputs_amount BIGINT, 
    outputs_address TEXT, 
    outputs_amount BIGINT, 
    block_num BIGINT NULL, 
    block_hash TEXT      NULL, 
    time timestamp with time zone NULL, 
    tx_state TEXT DEFAULT true, 
    tx_ordinal INTEGER,
    last_update timestamp with time zone, 
    tx_body TEXT      DEFAULT NULL
);

CREATE INDEX ON txs (hash);
CREATE INDEX ON txs (hash, last_update);
