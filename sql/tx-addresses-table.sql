CREATE TABLE tx_addresses ( 
    tx_hash TEXT REFERENCES txs ON DELETE CASCADE, 
    address TEXT, 
    PRIMARY KEY (tx_hash, address)
);

CREATE INDEX ON tx_addresses (tx_hash);
CREATE INDEX ON tx_addresses (address);
