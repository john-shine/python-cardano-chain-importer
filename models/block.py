from lib import utils
from cbor import cbor
from datetime import datetime

SLOTS_IN_EPOCH = 21600

class Block:

    def __init__(self, hash, slot, epoch, height, txs, isEBB, prevHash):
        self.hash = hash
        self.prevHash = prevHash
        self.slot = slot
        self.epoch = epoch
        self.height = height
        self.txs = txs
        self.isEBB = isEBB

    def serialize(self):
        return {
          'block_hash': self.hash,
          'epoch': self.epoch,
          'slot': self.slot,
          'block_height': self.height,
        }

    @staticmethod 
    def handle_epoch_boundary_block(header):
        [epoch, [chainDifficulty]] = header[3]
        return {
          'epoch': epoch,
          'height': chainDifficulty,
          'isEBB': True,
          'slot': None,
          'txs': None,
        }

    @staticmethod 
    def handle_regular_block(header, body: dict, blockHash: str, networkStartTime: int):
        consensus = header[3]
        [epoch, slot] = consensus[0]
        [chainDifficulty] = consensus[2]
        txs = body[0]
        [upd1, upd2] = body[3]
        blockTime = datetime.utcfromtimestamp(networkStartTime + (epoch * SLOTS_IN_EPOCH + slot) * 20) * 1000
        res = {
          'slot': slot,
          'epoch': epoch,
          'isEBB': False,
          'upd': [upd1, upd2] if (upd1.length or upd2.length) else None,
          'height': chainDifficulty,
          'txs': [utils.convert_raw_tx_to_obj(tx, {
              'txTime': blockTime,
              'txOrdinal': index,
              'blockNum': chainDifficulty,
              'blockHash': blockHash,
          }) for tx, index in txs]
        }
        return res

    @staticmethod 
    def parse_block(blob: bytes, handleRegularBlock: int):
        type, _ = cbor.loads(blob)
        header, body = _
        hash = utils.header_to_id(header, type)
        common = {
          'hash': hash,
          'magic': header[0],
          'prevHash': header[1].toString('hex'),
        }
        blockData = {}
        if type == 0:
            blockData.update(common)
            blockData.update(Block.handle_epoch_boundary_block(header))
        elif type == 1:
            blockData.update(common)
            blockData.update(Block.handle_regular_block(header, body, hash, handleRegularBlock))
        else:
            raise Exception(f'Unexpected block type! ${type}')

        return Block(blockData)

    @staticmethod
    def from_CBOR(data: bytes, handleRegularBlock: int):
        block = Block.parse_block(data, handleRegularBlock)
        return block
