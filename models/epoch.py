# // @flow
# import Block from './block'
from .block import Block

class Epoch:

    def __init__(self, data: any, network_start_time: int):
        # stip protocol magic
        self.data = data.buffer.slice(data.byteOffset,
          data.byteOffset + data.byteLength)
        self.network_start_time = network_start_time

    @staticmethod
    def from_CBOR(cls, data: bytes, network_start_time: int):
        return cls(data, network_start_time)

    @staticmethod
    def get_blockData_by_offset(self, blocks_list, offset: int):
        blockSize = DataView(blocks_list, offset).getUint32(0, False)
        blob = blocks_list.slice(offset + 4, offset + blockSize + 4)
        return [blockSize, Uint8Array(blob)]


    def get_next_block(self, blocks_list: bytes, offset: int):
        [blockSize, blob] = self.get_blockData_by_offset(blocks_list, offset)
        block = Block.from_CBOR(bytes.fromhex(blob), self.network_start_time)
        bytesToAllign = blockSize % 4
        nextBlockOffset = blockSize + 4 \
          + (bytesToAllign and (4 - bytesToAllign)) # 4 is block size field
        return [block, offset + nextBlockOffset]


    def get_blocks_iterator(self, options={}): 
        blocks_list = self.data.slice(16) # header
        nextBlock = lambda offset, number: self.get_next_block(blocks_list, offset)
        block = None
        offset = 0
        if options.get('omitEbb'):
          block, offset = nextBlock(offset)
          if not block.isEBB:
            yield block

        while offset < blocks_list.byteLength:
          [block, offset] = nextBlock(offset)
          yield block
