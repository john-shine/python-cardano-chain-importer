import asyncio
from time import time
from db import DB
from operator import itemgetter
from lib.logger import get_logger
from models.http_bridge import HttpBridge
from constants.scheduler import *


class Scheduler:

    error_meta = {
      'NODE_INACCESSIBLE': {
        'msg': 'node is inaccessible',
        'sleep': 60000
      },
      'ECONNREFUSED': {
        'msg': 'some unidentified network service is inaccessible',
        'sleep': 60000
      }
    }

    def __init__(self):
        self.logger = get_logger('scheduler')
        self.db = DB()
        self.http_bridge = HttpBridge()
        self.logger.info('check tip in every %d seconds. rollback count set to: %d blocks', CHECK_TIP_SECONDS, ROLLBACK_BLOCKS_COUNT)
        self.blocks_to_store = []
        self.last_block = {}

    async def rollback(self, at_block_height: int):
        self.logger.info(f'rollback at height {at_block_height} to {ROLLBACK_BLOCKS_COUNT} blocks back.')
        self.blocks_to_store = []
        self.last_block = {}
        try:
            # Recover database state to newest actual block.
            best_blockNum = await self.db.get_best_blockNum()
            height = best_blockNum['height']
            roll_back_to_height = height - ROLLBACK_BLOCKS_COUNT
            self.logger.info(f'current DB height at rollback time: {height}. rollback to: {roll_back_to_height}')
            await self.db.rollback_transactions(roll_back_to_height)
            await self.db.rollback_utxo_backup(roll_back_to_height)
            await self.db.rollback_block_history(roll_back_to_height)
            await self.db.update_best_blockNum(roll_back_to_height)
            best_blockNum = await self.db.get_best_blockNum()
            epoch, block_hash = itemgetter('epoch', 'hash')(best_blockNum)
            self.last_block = {'epoch': epoch, 'hash': block_hash}
        except Exception as e:
            raise

    async def process_epoch(self, epoch_id: int, height: int):
        self.logger.info(f'process epoch of: {epoch_id} in height: {height}')

        blocks = await self.http_bridge.get_parsed_epoch_by_id(epoch_id, True)
        for block in blocks:
            if block.height > height:
                await self.process_block(block)

    async def process_block_height(self, height: int):
        block = await self.http_bridge.get_block_by_height(height)
        return self.process_block(block, True)

    async def process_block(self, block, is_flush_cache=False):
        if self.last_block:
            if block.epoch == self.last_block['epoch'] and block.prev_hash != self.last_block['hash']:
                self.logger.info(f'block prev hash: {block.prev_hash} mismatch {self.last_block["hash"]}.  need rollback!')
                return STATUS_ROLLBACK_REQUIRED

        self.last_block = {
            'epoch': block.epoch,
            'hash': block.hash
        }

        self.blocks_to_store.append({
            'block_hash': block.hash,
            'block_height': block.height,
            'epoch': block.epoch,
            'slot': block.slot
        })

        try:
            block_have_txs = bool(block.txs)
            if block_have_txs:
                await self.db.store_block_txs(vars(block))

            if len(self.blocks_to_store) > BLOCKS_CACHE_SIZE or block_have_txs or is_flush_cache:
                await self.db.store_blocks(self.blocks_to_store)
                await self.db.update_best_blockNum(block.height)
                self.blocks_to_store = []
        except Exception as e:
            raise
        finally:
            if is_flush_cache or (block.height % LOG_BLOCK_PARSED_THRESHOLD == 0):
                self.logger.info(f'block parsed => hash: {block.hash} epoch: {block.epoch} slot: {block.slot} height: {block.height}')

        return STATUS_BLOCK_PROCESSED

    async def check_tip(self):
        self.logger.info('checking for new blocks.')
        best_blockNum = await self.db.get_best_blockNum()
        height, epoch, slot = itemgetter('height', 'epoch', 'slot')(best_blockNum)

        node_status = await self.http_bridge.get_status()
        packed_epochs, node_tip = itemgetter('packedEpochs', 'tip')(node_status)
        local_status = node_tip['local']
        remote_status = node_tip['remote']
        if not local_status:
            self.logger.info('cardano-http-brdige not synced yet')
            return

        self.logger.info(f'last imported block height: {height}. Node status => local: {local_status["slot"]}, remote: {remote_status["slot"]}, packed epochs: {packed_epochs}')

        remote_epoch, remote_slot = remote_status['slot']
        if epoch < remote_epoch:
            # If local epoch is lower than remote network tip, there's a potential for us to download full epochs, instead of single blocks
            # Calculate latest stable remote epoch
            last_remote_stable_epoch = remote_epoch - (1 if remote_slot > 2160 else 2)
            is_more_stable_epoch = epoch < last_remote_stable_epoch
            is_many_stable_slots = (epoch == last_remote_stable_epoch) and (slot < EPOCH_DOWNLOAD_THRESHOLD)
            # Check if there's any point to bother with whole epochs
            if is_more_stable_epoch or is_many_stable_slots:
                if packed_epochs > epoch:
                    for epoch_id in range(epoch, packed_epochs):
                        epoch_start_height = (height if epoch_id == epoch else 0)
                        await self.process_epoch(epoch_id, height)
                else:
                    self.logger.info(f'cardano-http-brdige has not yet packed stable epoch: {epoch}. last remote stable epoch is: {last_remote_stable_epoch}')
                return

        i = 0
        block_height = height + 1
        while True:
            if block_height <= local_status['height']:
                if i < MAX_BLOCKS_PER_LOOP:
                    break

            status = await self.process_block_height(block_height)
            if status == STATUS_ROLLBACK_REQUIRED:
                self.logger.info('rollback required.')
                await self.rollback(block_height)
                return

            i += 1
            block_height += 1

    async def start(self):
        self.logger.info('start chain syncing.')
        while True:
            time_start = time()
            error_sleep = 0
            try:
                await self.check_tip()
            except Exception as e:
                meta = None
                if hasattr(e, 'code'):
                    meta = self.error_meta.get(e.code)
                if meta:
                    error_sleep = meta['sleep']
                    self.logger.warn(f'Scheduler async: failed to check tip :: {meta["msg"]}. Sleeping and retrying (err_sleep={error_sleep})')
                else:
                    raise

            time_end = time()
            time_passed = time_end - time_start
            self.logger.info('chain sync loop finished in %d seconds', time_passed)

            time_sleep = error_sleep or (CHECK_TIP_SECONDS - time_passed)
            if time_sleep > 0:
                self.logger.info('sync loop sleep for %d seconds', time_sleep)
                await asyncio.sleep(time_sleep)
