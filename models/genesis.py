# import { helpers } from 'inversify-vanillajs-helpers'
# import { BLAKE2b } from 'bcrypto'
# import bs58 from 'bs58'
# import _ from 'lodash'
# import * as Cardano from 'cardano-wallet'
# import base64url from 'base64url'

# import {
#   RawDataProvider,
#   Logger,
#   Genesis,
# } from '../interfaces'
# import SERVICE_IDENTIFIER from '../constants/identifiers'

# import utils from '../blockchain/utils'
# import type { NetworkConfig } from '../interfaces'
import base58
from lib import utils
from hashlib import blake2b
from models.network import Network
from models.http_bridge import HttpBridge
from lib.logger import get_logger


class Genesis:

    def __init__(self):
        self.logger = get_logger('genesis')
        self.genesis_hash = Network().genesis_hash

    def non_avvm_balances_to_utxos(self, nonAvvmBalances):
        self.logger.debug('nonAvvmBalances to utxos')
        ret = []
        for non in nonAvvmBalances:
            amount, receiver = non
            utxoHash = utils.generate_utxo_hash(receiver)
            ret.append(utils.structUtxo(receiver, amount, utxoHash))

        return ret

    def avvm_distr_to_utxos(self, avvmDistr, protocolMagic):
        self.logger.debug('avvmDistrToUtxos called.')
        settings = Cardano.BlockchainSettings.from_json({
          'protocol_magic': protocolMagic,
        })
        ret = []
        for avv in avvmDistr:
            amount, publicRedeemKey = avv
            prk = Cardano.PublicRedeemKey.fromhex(base64url.decode(publicRedeemKey, 'hex'))
            receiver = prk.address(settings).to_base58()
            utxoHash = utils.generate_utxo_hash(receiver)
            ret.append(utils.structUtxo(receiver, amount, utxoHash))

        return ret
