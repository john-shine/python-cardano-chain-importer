# import cbor from 'cbor'
# import borc from 'borc'
# import bs58 from 'bs58'
# import blake from 'blakejs'
import base58
import json
from cbor import cbor
from config import config


def get_utxo_id(input):
    return f'${input["txId"]}${input["idx"]}'


def struct_utxo(receiver, amount, utxoHash, txIndex=0, blockNum=0):
    return {
        'utxo_id': f'${utxoHash}${txIndex}',
        'tx_hash': utxoHash,
        'tx_index': txIndex,
        'receiver': receiver,
        'amount': amount,
        'block_num': blockNum,
    }


"""
   * We need to use this function cuz there are some extra-long addresses
   * existing on Cardano mainnet. Some of them exceed 10K characters in length,
   * and Postgres can't store it.
   * We don't care about making these non-standard addresses spendable, so any address
   * over 1K characters is just truncated.
"""


def fix_long_address(address: str):
    if address and len(address) > 1000:
        return f'${address[0:497]}...${address[len(address) - 500:500]}'
    else:
        return address


def get_txs_utxos(txs):
    ret = {}
    for tx in txs:
        tx_id, outputs, blockNum = tx
        for index, output in enumerate(outputs):
            utxo = struct_utxo(
                fix_long_address(output['address']),
                output['value'],
                tx_id,
                index,
                blockNum
            )
            ret[f'${tx_id}${index}'] = utxo

    return ret


def decoded_tx_to_base(decodedTx):
    if isinstance(decodedTx, list):
        # eslint-disable-next-line default-case
        if len(decodedTx) == 2:
            signed = decodedTx
            return signed[0]
        elif len(decodedTx) == 3:
            base = decodedTx
            return base

    raise Exception(f'Unexpected decoded tx structure! ${json.dumps(decodedTx)}')


class CborIndefiniteLengthArray:
    elements = []
    cborEncoder = None

    def __init__(self, elements, cborEncoder):
        self.elements = elements
        self.cborEncoder = cborEncoder

    def encodeCBOR(self, encoder):
        elements = [bytes([0x9f])]
        for e in self.elements:
            elements.append(self.cborEncoder.encode(e))
        elements.append(bytes([0xff]))

        return elements


def select_cbor_encoder(outputs):
    maxAddressLen = 0
    for out in outputs:
        taggedAddress = out['taggedAddress']
        if len(taggedAddress['value']) > maxAddressLen:
            maxAddressLen = len(taggedAddress['value'])
    if maxAddressLen > 5000:
        self.logger.info('>>> Output address len exceeds maximum, using alternative CborEncoder')
        return borc

    return cbor


def pack_raw_txId_and_body(decodedTxBody):
    if not decodedTxBody:
        raise Exception('can not decode empty transaction!')

    try:
        inputs, outputs, attributes = decoded_tx_to_base(decodedTxBody)
        cborEncoder = select_cbor_encoder(outputs)
        enc = cborEncoder.encode([
            CborIndefiniteLengthArray(inputs, cborEncoder),
            CborIndefiniteLengthArray(outputs, cborEncoder),
            attributes,
        ])
        txId = blake.blake2bHex(enc, None, 32)
        txBody = enc.toString('hex')
        return [txId, txBody]
    except Exception as e:
        raise Exception(f'fail to convert raw transaction to ID! {str(e)}')


def raw_tx_to_obj(tx: list, extraData: dict):
    tx_inputs, tx_outputs, tx_witnesses = tx[0][0], tx[0][1], tx[1]
    txId, txBody = pack_raw_txId_and_body(tx)
    inputs, outputs, witnesses = [], [], []
    for inp in tx_inputs:
        types, tagged = inp
        inputTxId, idx = cbor.decode(tagged['value'])
        inputs.append({'type': types, 'txId': inputTxId.hex(), 'idx': idx})

    for out in tx_outputs:
        [address, value] = out
        outputs.append({'address': base58.b58encode(cbor.encode(address)), 'value': value})

    for w in tx_witnesses:
        [types, tagged] = w
        witnesses.append({'type': types, 'sign': cbor.decode(tagged.value)})

    ret = {
        'id': txId,
        'inputs': inputs,
        'outputs': outputs,
        'witnesses': witnesses,
        'txBody': txBody
    }
    ret.update(extraData)

    return ret


def header_to_id(header, type: int):
    headerData = cbor.loads([type, header])
    return blake.blake2bHex(headerData, None, 32)

def get_network_config(network_name):
    network = config.get('networks', {}).get(network_name)
    if not network:
        return None

    if not network.get('bridgeUrl'):
        network['bridgeUrl'] = config.get('defaultBridgeUrl')

    return network
