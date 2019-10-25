from lib.logger import get_logger
from lib import utils
import json
from constants.tx import TX_SUCCESS_STATUS
from datetime import datetime
import psycopg2
from config import config


class DB:

    def __init__(self):
        self._conn = None
        self.logger = get_logger('DB')

    @property
    def conn(self):
        if not self._conn:
            self._conn = psycopg2.connect(
                dbname=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                host=config['db']['host'],
                port=config['db']['port'],
                connect_timeout=config['db']['timeout']
            )

        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()

    async def store_utxos(self, utxos):
        sql = 'insert into utxos set ...'
        self.logger.info('store utxos', sql, utxos)
        await self.conn.execute(sql, utxos)
        return True

    async def get_best_blockNum(self):
        sql = 'select block_hash, block_height, epoch, slot from blocks order by block_height desc limit 1'
        row = await self.conn.fetchone(sql)
        if row:
            return {
                'hash': row['block_hash'],
                'height': row['block_height'],
                'epoch': row['epoch'],
                'slot': row['slot'],
            }

        return {'height': 0, 'epoch': 0, 'hash': None, 'slot': None}

    async def update_best_blockNum(self, best_blockNum: int):
        await self.conn.execute('update bestblock set best_block_num=%s', (best_blockNum))
        return True

    async def rollback_transactions(self, block_height: int):
        self.logger.info('rollbackTransactions to block ${block_height}')
        # sql = Q.sql.update()
        #   .table('txs')
        #   .set('tx_state', TX_STATUS.TX_PENDING_STATUS)
        #   .set('block_num', null)
        #   .set('block_hash', null)
        #   .set('time', null)
        #   .set('last_update', 'NOW()', { dontQuote: true })
        #   .where('block_num > ?', block_height)
        #   .toString()
        sql = 'update txs set tx_state=%s, block_num=%s, time=%s, last_update=%s where block_num>%s'
        await self.conn.execute(sql, (TX_PENDING_STATUS, None, None, None, datetime.now(), block_height))
        return True

    async def delete_invalid_utxos(self, block_height: int):
        self.logger.info('delete invalid utxos above block height: %s', block_height)
        sql = 'delete from utxos where block_num > %s'
        await self.conn.execute(sql, (block_height))

        sql = 'delete from utxos_backup where block_num > %s'
        await self.conn.execute(sql, (block_height))
        return True

    async def rollback_utxo_backup(self, block_height: int):
        self.logger.info('rollback utxo_backup to block height: %s', block_height)
        await self.delete_invalid_utxos(block_height)
        # sql = Q.sql.insert()
        #   .into('utxos')
        #   .with('moved_utxos',
        #     Q.sql.delete()
        #       .from('utxos_backup')
        #       .where('block_num < ?', block_height)
        #       .where('deleted_block_num > ?', block_height)
        #       .returning('*'))
        #   .fromQuery(['utxo_id', 'tx_hash', 'tx_index', 'receiver', 'amount', 'block_num'],
        #     Q.sql.select().from('moved_utxos')
        #       .field('utxo_id')
        #       .field('tx_hash')
        #       .field('tx_index')
        #       .field('receiver')
        #       .field('amount')
        #       .field('block_num'))
        #   .toString()

        sql = "insert into utxos values (select 'utxo_id', 'tx_hash', 'tx_index', 'receiver', 'amount', 'block_num' from moved_utxos)"
        db_res = await self.conn.execute(sql)
        return True

    async def rollback_block_history(self, block_height: int):
        self.logger.info('rollback block_history to block height: %s', block_height)

        await self.conn.query(' delete from blocks where block_height > %s', (block_height))
        return True

    async def store_block(self, block):
        if not block:
            return False

        sql = 'insert blocks set ' + '=?, '.join(block.keys()) + '=?'
        try:
            await self.conn.execute(sql, block.values())
        except Exception as e:
          self.logger.exception('Error occur on block: %s', block)
          return False

        return True

    async def store_blocks(self, blocks):
        if not blocks:
            return False

        blocks_data = []
        for block in blocks:
            blocks_data.append(block.serialize())

        sql = 'insert blocks set ' + '=?, '.join(block.keys()) + '=?'
        try:
          await self.conn.execute(sql, block.values())
        except Exception as e:
          self.logger.exception('Error occur on block', blocks)
          return False

        return True

    async def store_tx_addresses(self, tx_id: str, addresses: list):
        db_fields = [{
          'tx_hash': tx_id,
          'address': utils.fix_long_address(address),
        } for address in addresses]

        query = 'insert tx_addresses on conflict set ...'
        try:
            await self.conn.query(query)
        except Exception as e:
            self.logger.exception(f'addresses for ${tx_id} already stored')
            return False

        return True

    async def store_outputs(self, tx: dict):
        tx_id, outputs, blockNum = tx['id'], tx['outputs'], tx['blockNum']
        utxos_data = []
        for output, index in outputs:
            utxos_data.append(utils.structUtxo(
                utils.fixLongAddress(output.address), 
                output.value, 
                tx_id, 
                index, 
                blockNum
            ))

        await self.store_utxos(utxos_data)

    async def backup_and_remove_utxos(self, utxo_ids: list, deleted_blockNum: int):
        # query = Q.sql.insert()
        #   .into('utxos_backup')
        #   .with('moved_utxos',
        #     Q.sql.delete()
        #       .from('utxos')
        #       .where('utxo_id IN ?', utxo_ids)
        #       .returning('*'))
        #   .fromQuery([
        #     'utxo_id',
        #     'tx_hash',
        #     'tx_index',
        #     'receiver',
        #     'amount',
        #     'block_num',
        #     'deleted_block_num',
        #   ],
        #   Q.sql.select().from('moved_utxos')
        #     .field('utxo_id')
        #     .field('tx_hash')
        #     .field('tx_index')
        #     .field('receiver')
        #     .field('amount')
        #     .field('block_num')
        #     .field('${deleted_blockNum}', 'deleted_block_num'))
        #   .toString()
        query = ''
        self.logger.debug('backup and remove utxos: %s', query)
        await self.conn.query(query)
        return True

    async def get_utxos(self, utxo_ids: list):
        sql = 'select * from utxos where utxo_id in ($1)'
        rows = await self.conn.query(sql, utxo_ids)

        return [{
          'address': row['receiver'],
          'amount': row['amount'],
          'id': row['utxo_id'],
          'index': row['tx_index'],
          'txHash': row['tx_hash'],
        } for row in rows]

    async def get_outputs_for_tx_hashes(self, tx_hashes: list): 
        query = 'select from txs where hash in ?'
        rows = await self.conn.fetch(query, tx_hashes)
        pass
        # return db_res.rows.reduce((res, row) => {
        #   arr = _.map(_.zip(row.outputs_address, row.outputs_amount),
        #     ([address, amount]) => ({ address, amount }))
        #   res[row.hash] = arr
        #   return res
        # }, {})

    async def is_genesis_loaded(self):
        # Check whether utxo and blocks tables are empty.
        query = 'select (select count(*) from utxos) + (select count(*) from blocks) as cnt'
        count = await self.conn.fetchone(query)
        return count['cnt'] > 0

    async def store_tx(self, tx: dict, tx_utxos: dict):
        inputs, outputs, tx_id, blockNum, block_hash = tx['inputs'], tx['outputs'], tx['id'], tx['blockNum'], tx['block_hash']
        input_utxos = None
        self.logger.debug('store tx: %s', tx_utxos)
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
            'last_update': tx['txTime']
        }
        now = datetime.now()

        # query = Q.TX_INSERT.setFields(tx_db_fields)
        #   .onConflict('hash', {
        #     block_num: blockNum,
        #     block_hash: block_hash,
        #     time: tx.txTime,
        #     tx_state: txStatus,
        #     last_update: now,
        #     tx_ordinal: tx.txOrdinal,
        #   })
        #   .toString()

        query = 'insert into txs on conflict hash set block_num=?, block_hash=?, time=?, tx_state=?, last_update=?, tx_ordinal=?'

        self.logger.debug('insert into txs: %s', query)
        await self.conn.query(query)
        await self.store_tx_addresses(
            tx_id,
            list(set(input_addresses + output_addresses)),
        )

    async def store_block_txs(self, block):
        block_hash, epoch, slot, txs = block['hash'], block['epoch'], block['slot'], block['txs']
        self.logger.debug(f"store block txs (${epoch}/${slot}, ${block_hash}, ${block['height']})")
        new_utxos = utils.get_txs_utxos(txs)
        block_utxos = []
        required_inputs = []
        for tx in txs:
            for inp in tx['inputs']:
                utxo_id = utils.get_utxo_id(inp)
                local_utxo = new_utxos.get(utxo_id)
                if not local_utxo:
                    required_inputs.append(inp)
                else:
                    block_utxos.append({
                      'id': local_utxo.utxo_id,
                      'address': local_utxo.receiver,
                      'amount': local_utxo.amount,
                      'txHash': local_utxo.tx_hash,
                      'index': local_utxo.tx_index,
                    })
                    del new_utxos[utxo_id]

        required_utxo_ids = [utils.get_utxo_id(inp) for inp in required_inputs]
        self.logger.debug('store block txs required utxo', required_utxo_ids)
        available_utxos = await self.get_utxos(required_utxo_ids)
        all_utxo_map = available_utxos + block_utxos
        all_utxo_map.sort(key=lambda r: r['id'])
        # eslint-disable no-plusplus 
        for index in range(len(txs)):
            # eslint-disable no-await-in-loop 
            tx = txs[index]
            utxos = []
            for inp in tx['inputs']:
                utxo_id = utils.get_utxo_id(inp)
                if all_utxo_map.get(utxo_id):
                    utxos.append(all_utxo_map.get(utxo_id))

            if len(utxos) != len(tx['inputs']):
                raise Exception(f'failed to query input utxos for tx ${tx.id} for inputs: ${json.dumps(tx.inputs)} all utxos: ${json.dumps(all_utxo_map)}'
                )

            self.logger.debug('store block txs: %s', tx['id'])
            await self.store_tx(tx, utxos)

        await self.store_utxos(new_utxos.values())
        await self.backup_and_remove_utxos(required_utxo_ids, block['height'])
