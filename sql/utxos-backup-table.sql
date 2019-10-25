CREATE TABLE utxos_backup (
    like utxos including all,
    deleted_block_num integer
);