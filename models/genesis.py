# import base58
# import base64
from lib import utils
# from hashlib import blake2b
from models.network import Network
# from models.http_bridge import HttpBridge
from lib.logger import get_logger
from lib.utils import redeem_key_to_address


class Genesis:

    def __init__(self):
        self.logger = get_logger('genesis')
        self.genesis_hash = Network().genesis_hash

    def non_avvm_balances_to_utxos(self, non_avvm_balances):
        self.logger.debug('non avvm balances to utxos called.')
        ret = []
        for non_avvm in non_avvm_balances:
            amount, receiver_addr = non_avvm
            utxo_hash = utils.generate_utxo_hash(receiver_addr)
            ret.append(utils.struct_utxo(receiver_addr, amount, utxo_hash))

        return ret

    def avvm_distr_to_utxos(self, avvm_distr, protocol_magic):
        self.logger.debug('avvm distr to utxos called.')
        # settings = Cardano.BlockchainSettings.from_json({
        #   'protocol_magic': protocol_magic,
        # })
        ret = []
        for public_redeem_key, amount in avvm_distr.items():
            # prk = Cardano.PublicRedeemKey.fromhex(base64url.decode(public_redeem_key, 'hex'))
            # receiver_addr = prk.address(settings).to_base58()
            receiver_addr = redeem_key_to_address(public_redeem_key)
            utxo_hash = utils.generate_utxo_hash(receiver_addr)
            ret.append(utils.struct_utxo(receiver_addr, amount, utxo_hash))

        return ret
