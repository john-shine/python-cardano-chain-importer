from lib.logger import get_logger
from models.block import Block
from models.epoch import Epoch
from models.network import Network


class Parser:

    def __init__(self):
        self.logger = get_logger('parser')
        self.network_start_time = Network().start_time

    def parse_block(self, blob: bytes): 
        return Block.from_CBOR(blob, self.network_start_time)

    def parse_epoch(self, data: bytes, options={}):
        epoch = Epoch.from_CBOR(data, self.network_start_time)
        return epoch.get_blocks_iterator(options)
