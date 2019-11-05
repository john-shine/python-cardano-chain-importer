# // @flow
# import Block from './block'
from .block import Block

class Epoch:

    def __init__(self, data: any, network_start_time: int):
        self.data = data
        self.network_start_time = network_start_time

    @staticmethod
    def from_CBOR(data: bytes, network_start_time: int):
        return Epoch(data, network_start_time)

    @staticmethod
    def get_blockData_by_offset(blocks_list, offset: int):
        print('offset', offset)
        block_size = int.from_bytes(blocks_list[offset:offset + 4], byteorder='big')
        print('block_size', block_size)
        blob = blocks_list[offset + 4:offset + block_size + 4]
        return block_size, blob


    def get_next_block(self, blocks_list: bytes, offset: int):
        block_size, blob = self.get_blockData_by_offset(blocks_list, offset)
        block = Block.from_CBOR(blob, self.network_start_time)
        bytesToAllign = block_size % 4
        nextBlockOffset = block_size + 4 \
          + (bytesToAllign and (4 - bytesToAllign)) # 4 is block size field
        return block, offset + nextBlockOffset


    def get_blocks_iterator(self, options={}): 
        blocks_list = self.data[16:] # header
        nextBlock = lambda offset: self.get_next_block(blocks_list, offset)
        block = None
        offset = 0
        if options.get('omitEbb'):
          block, offset = nextBlock(offset)
          if not block.isEBB:
            yield block

        while offset < blocks_list.byteLength:
          [block, offset] = nextBlock(offset)
          yield block
