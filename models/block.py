from lib import utils
from cbor import cbor
from datetime import datetime

SLOTS_IN_EPOCH = 21600

class Block:

    def __init__(self, **kwargs):
        self.hash = kwargs['hash']
        self.prev_hash = kwargs['prev_hash']
        self.slot = kwargs['slot']
        self.epoch = kwargs['epoch']
        self.height = kwargs['height']
        self.txs = kwargs['txs']
        self.is_EBB = kwargs['is_EBB']

    def serialize(self):
        return {
          'block_hash': self.hash,
          'epoch': self.epoch,
          'slot': self.slot,
          'block_height': self.height,
        }

    @staticmethod 
    def handle_epoch_boundary_block(header):
        epoch, chain_difficulty = header[3]
        return {
          'epoch': epoch,
          'height': chain_difficulty[0],
          'is_EBB': True,
          'slot': None,
          'txs': None,
        }

    @staticmethod 
    def handle_regular_block(header: list, body: list, block_hash: str, network_start_time: int):
        consensus = header[3]
        epoch, slot = consensus[0]
        chain_difficulty,  = consensus[2]
        txs = body[0]
        upd1, upd2 = body[3]
        block_time = datetime.utcfromtimestamp(network_start_time + (epoch * SLOTS_IN_EPOCH + slot) * 20)

        return {
          'slot': slot,
          'epoch': epoch,
          'is_EBB': False,
          'upd': [upd1, upd2] if (len(upd1) or len(upd2)) else None,
          'height': chain_difficulty,
          'txs': [utils.convert_raw_tx_to_obj(tx, {
              'txTime': block_time,
              'txOrdinal': index,
              'blockNum': chain_difficulty,
              'block_hash': block_hash,
          }) for index, tx in enumerate(txs)]
        }

    @staticmethod 
    def parse_block(blob: bytes, handle_regular_block: int):
        block_type, _ = cbor.loads(blob)
        header, body, attrib = _
        hashs = utils.header_to_id(header, block_type)
        common = {
          'hash': hashs,
          'magic': header[0],
          'prev_hash': header[1].hex(),
        }
        block_data = {}
        if block_type == 0:
            block_data.update(common)
            block_data.update(Block.handle_epoch_boundary_block(header))
        elif block_type == 1:
            block_data.update(common)
            block_data.update(Block.handle_regular_block(header, body, hashs, handle_regular_block))
        else:
            raise Exception(f'unexpected block type: {block_type}')

        return Block(**block_data)

    @staticmethod
    def from_CBOR(data: bytes, handle_regular_block: int):
        block = Block.parse_block(data, handle_regular_block)
        return block
