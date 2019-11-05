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

    def auto_commit(self, is_auto=True):
        self.connect.autocommit = is_auto

    def close(self):
        if self._connect:
            self._connect.close()

    async def store_utxos(self, utxos):
        sql = 'insert into utxos (utxo_id, tx_hash, tx_index, receiver, amount, block_num) values %s'
        self.logger.info('store utxos: %s', len(utxos))
        with self.conn as cursor:
            execute_values(cursor, sql, utxos, "(%(utxo_id)s, %(tx_hash)s, %(tx_index)s, %(receiver)s, %(amount)s, %(block_num)s)")

        return True

    async def get_best_blockNum(self):
        sql = 'SELECT block_hash, block_height, epoch, slot FROM blocks ORDER BY block_height DESC LIMIT 1'
        with self.conn as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()

        if row:
            return {
                'hash': row['block_hash'],
                'height': row['block_height'],
                'epoch': row['epoch'],
                'slot': row['slot'],
            }

        return {'height': 0, 'epoch': 0, 'hash': None, 'slot': None}

    async def update_best_blockNum(self, best_blockNum: int):
        with self.conn as cursor:
            cursor.execute('update bestblock set best_block_num=%s', (best_blockNum, ))

        return True

    async def rollback_transactions(self, block_height: int):
        self.logger.info('rollbackTransactions to block: %s', block_height)
        sql = 'UPDATE txs '\
              'SET tx_state=%s, block_num=%s, time=%s, last_update=%s '\
              'WHERE block_num > %s'
        with self.conn as cursor:
            cursor.execute(sql, (TX_PENDING_STATUS, None, None, None, datetime.now(), block_height))

        return True

    async def delete_invalid_utxos(self, block_height: int):
        self.logger.info('delete invalid utxos above block height: %s', block_height)
        sql = 'delete FROM utxos where block_num > %s'
        with self.conn as cursor:
            cursor.execute(sql, (block_height, ))

        sql = 'delete FROM utxos_backup where block_num > %s'
        with self.conn as cursor:
            cursor.execute(sql, (block_height, ))

        return True

    async def rollback_utxo_backup(self, block_height: int):
        self.logger.info('rollback utxo_backup to block height: %s', block_height)
        await self.delete_invalid_utxos(block_height)

        sql = 'WITH moved_utxos AS (DELETE FROM utxos_backup WHERE block_num < %s AND delete_block_num > %s RETURNING *) INSERT INTO utxos SELECT * FROM moved_utxos'
        with self.conn as cursor:
            cursor.execute(sql, (block_height, block_height))

        return True

    async def rollback_block_history(self, block_height: int):
        self.logger.info('rollback block_history to block height: %s', block_height)

        with self.conn as cursor:
            cursor.execute('delete FROM blocks where block_height > %s', (block_height, ))

        return True

    async def store_block(self, block):
        if not block:
            return False

        sql = 'INSERT INTO blocks (block_hash, block_height, epoch, slot) VALUES (%(block_hash)s, %(block_height)s, %(epoch)s, %(slot)s)'
        try:
            with self.conn as cursor:
                cursor.execute(sql, vars(block))
        except Exception as e:
            self.logger.exception('error occur on block: %s', block)
            return False

        return True

    async def store_blocks(self, blocks):
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
            self.logger.exception('error occur on %s blocks', len(blocks))
            return False

        return True

    async def store_tx_addresses(self, tx_id: str, addresses: list):
        db_fields = [{
          'tx_hash': tx_id,
          'address': utils.fix_long_address(address),
        } for address in addresses]

        query = 'insert tx_addresses on conflict set ...'
        try:
            with self.conn as cursor:
                cursor.execute(query)
        except Exception as e:
            self.logger.exception('addresses for %s already stored', tx_id)
            return False

        return True

    async def store_outputs(self, tx: dict):
        tx_id, outputs, blockNum = tx['id'], tx['outputs'], tx['blockNum']
        utxos_data = []
        for output, index in outputs:
            utxos_data.append(utils.struct_utxo(
                utils.fix_long_address(output.address),
                output.value, 
                tx_id, 
                index, 
                blockNum
            ))

        await self.store_utxos(utxos_data)

    async def backup_and_remove_utxos(self, utxo_ids: list, deleted_block_num: int):
        if not utxo_ids:
            return False
        
        sql = 'WITH moved_utxos AS (DELETE FROM utxos WHERE utxo_id IN (%s)) '\
              '  INSERT INTO utxos_backup '\
              '  (utxo_id, tx_hash, tx_index, receiver, amount, block_num, deleted_block_num) '\
              '  (SELECT utxo_id, tx_hash, tx_index, receiver, amount, block_num, %s AS deleted_block_num FROM moved_utxos)'
        str_ids = ', '.join(utxo_ids)
        with self.conn as cursor:
            cursor.execute(sql, (str_ids, deleted_block_num))
            self.logger.info('backup and remove utxos: %s', str_ids)

        return True

    async def get_utxos(self, utxo_ids: list):
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

    async def get_outputs_for_tx_hashes(self, tx_hashes: list):
        if not tx_hashes:
            return {}

        sql = 'SELECT FROM txs where hash in (%s)'
        with self.conn as cursor:
            cursor.execute(sql, (', '.join(tx_hashes), ))
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

    async def store_tx(self, tx: dict, tx_utxos: dict):
        inputs, outputs, tx_id, blockNum, block_hash = tx['inputs'], tx['outputs'], tx['id'], tx['blockNum'], tx['block_hash']
        input_utxos = None
        self.logger.info('store tx: %s', tx_utxos)
        tx_status = tx['status'] if tx['status'] else TX_SUCCESS_STATUS
        if not tx_utxos:
          input_utxo_ids = []
          for _input in inputs:
              input_utxo_ids.append(utils.get_utxo_id())

          input_utxos = await self.get_utxos(input_utxo_ids)
        else:
          input_utxos = tx_utxos

        input_addresses = [inp['address'] for inp in input_utxos]
        output_addresses = [utils.fix_long_address(out['address']) for out in outputs]
        input_ammounts = [int(item['amount']) for item in input_utxos]
        output_ammounts = [int(item['value']) for item in outputs]
        tx_db_fields = {
            'hash': tx_id,
            'inputs': json.dumps(input_utxos),
            'inputs_address': input_addresses,
            'inputs_amount': input_ammounts,
            'outputs_address': output_addresses,
            'outputs_amount': output_ammounts,
            'block_num': blockNum,
            'block_hash': block_hash,
            'tx_state': tx_status,
            'tx_body': tx['txBody'],
            'tx_ordinal': tx['txOrdinal'],
            'time': tx['txTime'],
            'last_update': datetime.now()
        }

        sql = 'INSERT INTO txs ({}) VALUSE ({}) '\
              'ON CONFLICT (hash) DO UPDATE '\
              'SET block_num=EXCLUDED.block_num, '\
              '    block_hash=EXCLUDED.block_hash, '\
              '    time=EXCLUDED.time, '\
              '    tx_state=EXCLUDED.tx_state, '\
              '    last_update=EXCLUDED.last_update, '\
              '    tx_ordinal=EXCLUDED.tx_ordinal'.format(
                ', '.join(tx_db_fields.keys()),
                ', '.join(['%s'] * len(tx_db_fields))
            )

        self.logger.info('insert into txs: %s', sql)
        with self.conn as cursor:
            cursor.execute(sql, (tx_db_fields.values(), ))

        await self.store_tx_addresses(
            tx_id,
            list(set(input_addresses + output_addresses)),
        )

    async def store_block_txs(self, block: dict):
        block_hash, epoch, slot, txs = itemgetter('hash', 'epoch', 'slot', 'txs')(block)
        self.logger.info('store txs for block height: %s', block['height'])
        txs_utxos = utils.get_txs_utxos(txs)
        block_utxos, required_utxo_ids = [], []
        for tx in txs:
            for inp in tx['inputs']:
                utxo_id = utils.get_utxo_id(inp)
                local_utxo = txs_utxos.get(utxo_id)
                if local_utxo:
                    block_utxos.append({
                        'id': local_utxo['utxo_id'],
                        'address': local_utxo['receiver'],
                        'amount': local_utxo['amount'],
                        'txHash': local_utxo['tx_hash'],
                        'index': local_utxo['tx_index'],
                    })
                    del txs_utxos[utxo_id]
                else:
                    required_utxo_ids.append(utxo_id)

        self.logger.info('store block txs required utxo: %s', required_utxo_ids)
        available_utxos = await self.get_utxos(required_utxo_ids)
        all_utxo_map = {}
        for utxo in available_utxos + block_utxos:
            all_utxo_map[utxo['id']] = utxo

        for index, tx in enumerate(txs):
            utxos = []
            for inp in tx['inputs']:
                utxo_id = utils.get_utxo_id(inp)
                utxo = all_utxo_map.get(utxo_id)
                if utxo:
                    utxos.append(utxo)

            if len(utxos) != len(tx['inputs']):
                raise Exception(f'failed to query input utxos for tx: {tx["id"]} in db or block.')

            self.logger.info('store block txs: %s', tx['id'])
            await self.store_tx(tx, utxos)

        await self.store_utxos(list(txs_utxos.values()))
        await self.backup_and_remove_utxos(required_utxo_ids, block['height'])
