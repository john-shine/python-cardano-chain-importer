# import cbor from 'cbor'
# import borc from 'borc'
# import bs58 from 'bs58'
# import blake from 'blakejs'
import json
import base58
from cbor import cbor
from hashlib import blake2b
from config import config


def generate_utxo_hash(address):
  data = base58.b58decode(address)
  return blake2b(data).hexdigest()


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


def decoded_tx_to_base(decoded_tx):
    if isinstance(decoded_tx, list):
        if len(decoded_tx) == 2:
            signed = decoded_tx
            return signed[0]
        elif len(decoded_tx) == 3:
            base = decoded_tx
            return base

    raise Exception('invalid decoded tx structure: %s' % decoded_tx)


class CborIndefiniteLengthArray:

    def __call__(self, elements):
        ret = [bytes([0x9f])]
        for e in elements:
            ret.append(cbor.dumps(e))
        ret.append(bytes([0xff]))

        return ret


def pack_raw_txid_and_body(decoded_tx_body):
    if not decoded_tx_body:
        raise Exception('can not decode empty tx!')

    try:
        inputs, outputs, attributes = decoded_tx_to_base(decoded_tx_body)
        cbor_indef_array = CborIndefiniteLengthArray()
        enc = cbor.dumps([
            cbor_indef_array(inputs),
            cbor_indef_array(outputs),
            attributes,
        ])
        tx_id = blake2b(enc, digest_size=32).hexdigest()
        tx_body = enc.hex()

        return [tx_id, tx_body]
    except Exception as e:
        raise Exception(f'fail to convert raw tx to ID! {str(e)}')


def convert_raw_tx_to_obj(tx: list, extraData: dict):
    tx_inputs, tx_outputs, tx_witnesses = tx[0][0], tx[0][1], tx[1]
    tx_id, tx_body = pack_raw_txid_and_body(tx)
    inputs, outputs, witnesses = [], [], []
    for inp in tx_inputs:
        types, tagged = inp
        inputTxId, idx = cbor.loads(tagged.value)
        inputs.append({'type': types, 'txId': inputTxId.hex(), 'idx': idx})

    for out in tx_outputs:
        address, value = out
        outputs.append({'address': base58.b58encode(cbor.dumps(address)), 'value': value})

    for wit in tx_witnesses:
        types, tagged = wit
        witnesses.append({'type': types, 'sign': cbor.loads(tagged.value)})

    ret = {
        'id': tx_id,
        'inputs': inputs,
        'outputs': outputs,
        'witnesses': witnesses,
        'txBody': tx_body
    }
    ret.update(extraData)

    return ret


def header_to_id(header, tx_type: int):
    header_data = cbor.loads([tx_type, header])
    return blake2b(header_data, digest_size=32).hexdigest()

def get_network_config(network_name):
    network = config.get('networks', {}).get(network_name)
    if not network:
        return None

    if not network.get('bridgeUrl'):
        network['bridgeUrl'] = config.get('defaultBridgeUrl')

    return network
