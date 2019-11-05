from .block import Block

class Epoch:

    def __init__(self, data: any, network_start_time: int):
        self.data = data
        self.network_start_time = network_start_time

    @staticmethod
    def from_CBOR(data: bytes, network_start_time: int):
        return Epoch(data, network_start_time)

    @staticmethod
    def get_blockdata_by_offset(blocks_list, offset: int):
        block_size = int.from_bytes(blocks_list[offset:(offset + 4)], byteorder='big')
        blob = blocks_list[(offset + 4):(offset + block_size + 4)]
        return block_size, blob


    def get_next_block(self, blocks_list: bytes, offset: int):
        block_size, blob = self.get_blockdata_by_offset(blocks_list, offset)
        block = Block.from_CBOR(blob, self.network_start_time)
        bytes_to_allign = block_size % 4
        next_block_offset = block_size + 4 \
          + (bytes_to_allign and (4 - bytes_to_allign)) # 4 is block size field
        return block, offset + next_block_offset


    def get_blocks_iterator(self, options={}): 
        blocks_list = self.data[16:] # header
        next_block = lambda offset: self.get_next_block(blocks_list, offset)
        block = None
        offset = 0
        if options.get('omitEbb'):
            block, offset = next_block(offset)
            if not block.is_EBB:
                yield block

        while offset < len(blocks_list):
            block, offset = next_block(offset)
            yield block
