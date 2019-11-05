import asyncio
from time import time
from db import DB
from operator import itemgetter
from lib.logger import get_logger
from models.http_bridge import HttpBridge
from constants.network import *

# import {
#   Scheduler,
#   RawDataProvider,
#   Database,
#   Logger,
# } from '../interfaces'
# import SERVICE_IDENTIFIER from '../constants/identifiers'
# import Block from '../blockchain'


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

    STATUS_ROLLBACK_REQUIRED = 'ROLLBACK_REQUIRED'
    BLOCK_STATUS_PROCESSED = 'BLOCK_PROCESSED'

    def __init__(self, check_tip_seconds=15, rollback_blocks_count=25):
        self.logger = get_logger('scheduler')
        self.db = DB()
        self.http_bridge = HttpBridge()
        self.rollback_blocks_count = rollback_blocks_count
        self.check_tip_seconds = check_tip_seconds
        self.logger.debug('check tip in every %d seconds', self.check_tip_seconds)
        self.logger.debug('rollback blocks count set to: %d', self.rollback_blocks_count)
        self.blocks_to_store = []
        self.last_block = {'epoch': None, 'hash': None}

    async def rollback(self, at_block_height: int):
        self.logger.info(f'rollback at height {at_block_height} to {self.rollback_blocks_count} blocks back.')
        # reset scheduler state
        self.blocks_to_store = []
        self.last_block = {}
        self.db.auto_commit(False)
        try:
            # Recover database state to newest actual block.
            best_blockNum = await self.db.get_best_blockNum()
            height = best_blockNum['height']
            roll_back_to_height = height - self.rollback_blocks_count
            self.logger.info(f'current DB height at rollback time: {height}. rollback to: {roll_back_to_height}')
            await self.db.rollback_transactions(roll_back_to_height)
            await self.db.rollback_utxo_backup(roll_back_to_height)
            await self.db.rollback_block_history(roll_back_to_height)
            await self.db.update_best_blockNum(roll_back_to_height)
            best_blockNum = await self.db.get_best_blockNum()
            epoch, block_hash = itemgetter('epoch', 'hash')(best_blockNum)
            self.last_block = {'epoch': epoch, 'hash': block_hash}
            self.db.conn.commit()
        except Exception as e:
            self.db.conn.rollback()
            raise

    async def process_epoch(self, epoch_id: int, height: int):
        self.logger.info(f'process epoch of id: {epoch_id}, {height}')

        blocks = await self.http_bridge.get_parsed_epoch_by_id(epoch_id, True)
        for block in blocks:
            if block.height > height:
                await self.process_block(block)

    async def process_block_height(self, height: int):
        block = await self.http_bridge.get_block_by_height(height)
        is_flush_cache = True
        return self.process_block(block, is_flush_cache)

    async def process_block(self, block, is_flush_cache=False):
        if (self.last_block and block.epoch == self.last_block['epoch']
          and block.prev_hash != self.last_block['hash']):
            self.logger.info(f'({block.epoch}/{block.slot}) block.prev_hash ({block.prev_hash}) != last_block["hash"] ({self.last_block["hash"]}). Performing rollback...')
            return self.STATUS_ROLLBACK_REQUIRED

        self.last_block = {
            'epoch': block.epoch,
            'hash': block.hash
        }

        block_have_txs = bool(block.txs)
        self.blocks_to_store.append(block)
        self.db.auto_commit(False)
        try:
            if (len(self.blocks_to_store) > BLOCKS_CACHE_SIZE or block_have_txs or is_flush_cache):
                if block_have_txs:
                    await self.db.store_block_txs(block)
        
                await self.db.store_block(self.blocks_to_store)
                await self.db.update_best_blockNum(block.height)
                self.blocks_to_store = []
                self.db.conn.commit()
        except Exception as e:
            self.db.conn.rollback()
            raise
        finally:
            if is_flush_cache or (block.height % LOG_BLOCK_PARSED_THRESHOLD == 0):
                self.logger.info(f'block parsed: {block.hash} {block.epoch} {block.slot} {block.height}')

        return self.BLOCK_STATUS_PROCESSED

    async def check_tip(self):
        self.logger.info('checking for new blocks.')
        self.db.auto_commit(True)
        best_blockNum = await self.db.get_best_blockNum()
        height, epoch, slot = itemgetter('height', 'epoch', 'slot')(best_blockNum)

        node_status = await self.http_bridge.get_status()
        packed_epochs, node_tip = itemgetter('packedEpochs', 'tip')(node_status)
        tip_status = node_tip['local']
        remote_status = node_tip['remote']
        if not tip_status:
            self.logger.info('cardano-http-brdige not synced yet')
            return

        self.logger.info(f'last imported block {height}. Node status: local={tip_status["slot"]} remote={remote_status["slot"]} packed_epochs={packed_epochs}')

        [remote_epoch, remote_slot] = remote_status['slot']
        if epoch < remote_epoch:
            # If local epoch is lower than the current network tip, there's a potential for us to download full epochs, instead of single blocks
            # Calculate latest stable remote epoch
            last_remote_stable_epoch = remote_epoch - (1 if remote_slot > 2160 else 2)
            is_more_stable_epoch = epoch < last_remote_stable_epoch
            is_many_stable_slots = (epoch == last_remote_stable_epoch) and (slot < EPOCH_DOWNLOAD_THRESHOLD)
            # Check if there's any point to bother with whole epochs
            if is_more_stable_epoch or is_many_stable_slots:
                if packed_epochs > epoch:
                      for epoch_id in range(epoch, packed_epochs):
                        epoch_start_height = (height if epoch_id == epoch else 0)
                        # Process epoch
                        await self.process_epoch(epoch_id, height)
                else:
                  # Packed epoch is not available yet
                  self.logger.info('cardano-http-brdige has not yet packed stable epoch: {epoch} (last_remote_stable_epoch={last_remote_stable_epoch})')

                return

        i = 0
        block_height = height + 1
        while True:
            if (block_height <= tip_status['height']) and (i < MAX_BLOCKS_PER_LOOP):
                break

            status = await self.process_block_height(block_height)
            if status == self.STATUS_ROLLBACK_REQUIRED:
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

            time_sleep = error_sleep or (self.check_tip_seconds - time_passed)
            if time_sleep > 0:
                self.logger.info('sync loop sleep for %d seconds', time_sleep)
                await asyncio.sleep(time_sleep)
