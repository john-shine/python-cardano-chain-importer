from lib.logger import get_logger
from lib import utils
import json
from datetime import datetime
import psycopg2
from config import config
from operator import itemgetter
from psycopg2.extras import RealDictCursor, execute_values
from constants.transaction import TX_SUCCESS_STATUS, TX_PENDING_STATUS


class DB:

    def __init__(self):
        self._cursor = None
        self._connect = None
        self.logger = get_logger('DB')

    @property
    def conn(self):
        if not self._cursor or self._cursor.closed:
            self._cursor = self.connect.cursor(cursor_factory=RealDictCursor)

        return self._cursor

    @property
    def connect(self):
        if not self._connect:
            self._connect = psycopg2.connect(
                dbname=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                host=config['db']['host'],
                port=config['db']['port'],
                connect_timeout=config['db']['timeout']
            )
            self._connect.autocommit = True

        return self._connect

    # def auto_commit(self, is_auto=True):
    #     self.connect.autocommit = is_auto

    def close(self):
        if self._connect:
            self._connect.close()

    async def save_utxos(self, utxos: list):
        sql = 'INSERT INTO utxos '\
              '(utxo_id, tx_hash, tx_index, receiver, amount, block_num) values %s '\
              'ON CONFLICT (utxo_id) DO UPDATE '\
              'SET tx_hash=EXCLUDED.tx_hash, '\
              '    tx_index=EXCLUDED.tx_index, '\
              '    receiver=EXCLUDED.receiver, '\
              '    amount=EXCLUDED.amount, '\
              '    block_num=EXCLUDED.block_num'
        self.logger.info('store %d utxos in db', len(utxos))
        with self.conn as cursor:
            execute_values(cursor, sql, utxos, "(%(utxo_id)s, %(tx_hash)s, %(tx_index)s, %(receiver)s, %(amount)s, %(block_num)s)")

        return True

    async def get_best_block_num(self):
        sql = 'SELECT block_hash, block_height, epoch, slot FROM blocks ORDER BY block_height DESC LIMIT 1'
        with self.conn as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()

        if not row:
            return {'height': 0, 'epoch': 0, 'hash': None, 'slot': None}

        return {
            'hash': row['block_hash'],
            'height': row['block_height'],
            'epoch': row['epoch'],
            'slot': row['slot']
        }

    async def update_best_block_num(self, best_block_num: int):
        self.logger.info('update best block num in db to: %d', best_block_num)
        with self.conn as cursor:
            cursor.execute('UPDATE bestblock SET best_block_num=%s', (best_block_num, ))

        return True

    async def rollback_txs_from_height(self, block_height: int):
        self.logger.info('rollback  transactions from block height: %s', block_height)
        sql = 'UPDATE txs '\
              'SET tx_state=%s, block_num=%s, time=%s, last_update=%s '\
              'WHERE block_num > %s'
        with self.conn as cursor:
            data = TX_PENDING_STATUS, None, None, None, datetime.now(), block_height
            cursor.execute(sql, data)

        return True

    async def delete_invalid_utxos_and_backup(self, block_height: int):
        self.logger.info('delete invalid utxos from block height: %s', block_height)
        sql = 'DELETE FROM utxos WHERE block_num > %s'
        with self.conn as cursor:
            cursor.execute(sql, (block_height, ))

        sql = 'DELETE FROM utxos_backup WHERE block_num > %s'
        with self.conn as cursor:
            cursor.execute(sql, (block_height, ))

        return True

    async def rollback_utxos_backup(self, block_height: int):
        self.logger.info('rollback utxo_backup to block height: %s', block_height)
        await self.delete_invalid_utxos_and_backup(block_height)

        sql = 'WITH moved_utxos AS ('\
              '  DELETE FROM utxos_backup '\
              '  WHERE block_num < %s AND delete_block_num > %s RETURNING *'\
              ') '\
              'INSERT INTO utxos SELECT * FROM moved_utxos'
        with self.conn as cursor:
            cursor.execute(sql, (block_height, block_height))

        return True

    async def rollback_blocks_from_height(self, block_height: int):
        self.logger.info('rollback block_history to block height: %s', block_height)

        with self.conn as cursor:
            cursor.execute('DELETE FROM blocks WHERE block_height > %s', (block_height, ))

        return True

    async def save_block(self, block):
        if not block:
            return False

        sql = 'INSERT INTO blocks (block_hash, block_height, epoch, slot) VALUES '\
              '(%(block_hash)s, %(block_height)s, %(epoch)s, %(slot)s)'
        try:
            with self.conn as cursor:
                cursor.execute(sql, vars(block))
        except Exception as e:
            self.logger.exception('error on save block: %s', block)
            return False

        return True

    async def save_blocks(self, blocks):
        if not blocks:
            return False

        sql = 'INSERT INTO blocks (block_hash, block_height, epoch, slot) VALUES %s'
        try:
            with self.conn as cursor:
                execute_values(
                    cursor, 
                    sql, 
                    blocks, 
                    "(%(block_hash)s, %(block_height)s, %(epoch)s, %(slot)s)"
                )
        except Exception as e:
            self.logger.exception('error on save %s blocks', len(blocks))
            return False

        return True

    async def save_tx_addresses(self, tx_id: str, addresses: list):
        db_fields = [{
          'tx_hash': tx_id,
          'address': utils.fix_long_address(address),
        } for address in addresses]

        query = 'INSERT INTO tx_addresses (tx_hash, address) VALUES %s '\
                'ON CONFLICT (tx_hash, address) DO UPDATE '\
                'SET tx_hash=EXCLUDED.tx_hash, address=EXCLUDED.address'
        try:
            with self.conn as cursor:
                execute_values(cursor, query, db_fields, "(%(tx_hash)s, %(address)s)")
        except Exception as e:
            self.logger.exception('addresses for %s already stored', tx_id)
            return False

        return True

    async def remove_and_backup_utxos(self, utxo_ids: list, deleted_block_num: int):
        if not utxo_ids:
            return False
        
        sql = 'WITH moved_utxos AS (DELETE FROM utxos WHERE utxo_id IN (%s) RETURNING *) '\
              '  INSERT INTO utxos_backup '\
              '  (utxo_id, tx_hash, tx_index, receiver, amount, block_num, deleted_block_num) '\
              '  (SELECT utxo_id, tx_hash, tx_index, receiver, amount, block_num, %s AS deleted_block_num FROM moved_utxos)'
        str_ids = ', '.join(utxo_ids)
        with self.conn as cursor:
            cursor.execute(sql, (str_ids, deleted_block_num))
            self.logger.info('backup and remove utxos: %s', str_ids)

        return True

    async def get_utxos_by_ids(self, utxo_ids: list):
        if not utxo_ids:
            return []

        sql = 'SELECT * FROM utxos WHERE utxo_id IN (\'{}\')'
        with self.conn as cursor:
            cursor.execute(sql.format('\', \''.join(utxo_ids)))
            rows = cursor.fetchall()

        return [{
          'address': row['receiver'],
          'amount': row['amount'],
          'id': row['utxo_id'],
          'index': row['tx_index'],
          'txHash': row['tx_hash'],
        } for row in rows]

    async def get_txs_by_hashes(self, tx_hashes: list):
        if not tx_hashes:
            return {}

        sql = 'SELECT * FROM txs where hash in (\'{}\')'
        with self.conn as cursor:
            cursor.execute(sql.format('\', \''.join(tx_hashes)))
            rows = cursor.fetchall()

        res = {}
        for row in rows:
            res[row['hash']] = (row['address'], row['amount'])

        return res

    async def is_genesis_loaded(self):
        # Check whether utxo and blocks tables are empty.
        query = 'SELECT (SELECT count(*) FROM utxos) + (SELECT count(*) FROM blocks) as cnt'
        with self.conn as cursor:
            cursor.execute(query)
            count = cursor.fetchone()

        return count['cnt'] > 0

    async def convert_txs(self, tx: dict, tx_utxos: dict):
        inputs, outputs, tx_id, block_num, block_hash = tx['inputs'], tx['outputs'], tx['id'], tx['blockNum'], tx['block_hash']
        self.logger.info('store tx: %s', tx_utxos)
        if not tx_utxos:
            input_utxo_ids = []
            for inp in inputs:
                input_utxo_ids.append(utils.get_utxo_id(inp))

            input_utxos = await self.get_utxos_by_ids(input_utxo_ids)
        else:
            input_utxos = tx_utxos

        input_addresses = [inp['address'] for inp in input_utxos]
        output_addresses = [utils.fix_long_address(out['address']) for out in outputs]
        input_ammounts = [int(item['amount']) for item in input_utxos]
        output_ammounts = [int(item['value']) for item in outputs]
        return {
            'hash': tx_id,
            'inputs': json.dumps(input_utxos),
            'inputs_address': input_addresses,
            'inputs_amount': input_ammounts,
            'outputs_address': output_addresses,
            'outputs_amount': output_ammounts,
            'block_num': block_num,
            'block_hash': block_hash,
            'tx_state': tx['status'] if tx.get('status') else TX_SUCCESS_STATUS,
            'tx_body': tx['txBody'],
            'tx_ordinal': tx['txOrdinal'],
            'time': tx['txTime'],
            'last_update': datetime.now()
        }

    async def save_txs(self, tx: dict, tx_utxos: dict):
        tx_db_fields = await self.convert_txs(tx, tx_utxos)

        sql = 'INSERT INTO txs ({}) VALUES ({}) '\
              'ON CONFLICT (hash) DO UPDATE '\
              'SET block_num=EXCLUDED.block_num, '\
              '    block_hash=EXCLUDED.block_hash, '\
              '    time=EXCLUDED.time, '\
              '    tx_state=EXCLUDED.tx_state, '\
              '    last_update=EXCLUDED.last_update, '\
              '    tx_ordinal=EXCLUDED.tx_ordinal'
        sql = sql.format(
            ', '.join(tx_db_fields.keys()), 
            ', '.join(['%s'] * len(tx_db_fields))
        )

        self.logger.info('insert into txs: %s', sql)
        with self.conn as cursor:
            cursor.execute(sql, tuple(tx_db_fields.values()))

        addresses = list(set(input_addresses + output_addresses))
        await self.save_tx_addresses(tx_id, addresses)
