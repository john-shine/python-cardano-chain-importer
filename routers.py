# import cbor from 'cbor'
# import bs58 from 'bs58'
# import blake from 'blakejs'
# import _ from 'lodash'
# # eslint-disable-next-line camelcase
# import { sha3_256 } from 'js-sha3'

# import { Request, Response } from 'restify'
# import { Controller, Post } from 'inversify-restify-utils'
# import { Controller as IController } from 'inversify-restify-utils/lib/interfaces'
# import { injectable, decorate, inject } from 'inversify'

# import { Logger, RawDataProvider, Database, NetworkConfig } from '../interfaces'
# import SERVICE_IDENTIFIER from '../constants/identifiers'
# import utils from '../blockchain/utils'
# import { TX_STATUS, TxType } from '../blockchain'
import json
import base58
import base64
from db import DB
from cbor import cbor
from constants.transaction import *
from datetime import datetime
from lib import utils
from hashlib import blake2b, sha3_256
from lib.logger import get_logger
from tornado.web import RequestHandler
from models.network import Network
from models.http_bridge import HttpBridge


class Routers:

    def __call__(self):
        return [
            (r'/api/txs/signed', self.SignHandler)
        ]

    @classmethod
    def fail(cls, self, message):
        return self.write(json.dumps({'success': False, 'message': message}))

    @classmethod
    def success(cls, self):
        return self.write(json.dumps({'success': True, 'message': 'OK'}))

    class SignHandler(RequestHandler):

        def initialize(self):
            self.logger = get_logger('routers')
            self.http_bridge = HttpBridge()
            self.db = DB()
            self.expected_network_magic = Network().network_magic

        def set_default_headers(self):
            self.set_header("Content-Type", 'application/json')

        async def post(self):
            try:
                body = json.loads(self.request.body)
            except json.decoder.JSONDecodeError:
                return Routers.fail(self, 'invalid request')

            if not isinstance(body, dict):
                return Routers.fail(self, 'invalid request')

            tx_payload = body.get('signedTx')
            if not tx_payload:
                return Routers.fail(self, 'request signedTx is empty')

            tx_obj = self.parse_raw_tx(tx_payload)
            validate_error = await self.validate_tx(tx_obj)
            if validate_error:
                self.logger.error('local tx validation failed: %s', validate_error)

            try:            
                resp = await self.http_bridge.post_signed_tx(json.dumps(body))
            except Exception as e:
                self.logger.exception(e)
                return Routers.fail(self, 'send tx to bridge error')

            self.logger.debug('send tx response: %s', resp)
            try:
                if resp.status == 200:
                    await self.store_tx_as_pending(tx_obj)
                    if validate_error:
                        self.logger.warn('local validation error, but network send succeed!')
            except Exception as e:
                self.logger.exception('fail to store tx as pending in DB!');
                raise Exception('Internal DB fail in the importer!')

            status_text, status, resp_body = None, None, None
            if validate_error and resp.status != 200:
                # We send specific local response with network response attached
                ret = f'Transaction validation error: {validate_error} (Network response: {resp.data}).'
                return Routers.fail(self, ret)
            else:
                # Locally we have no validation errors - proxy the network response
                if resp.status == 200:
                    return Routers.success(self)

                return Routers.fail(resp.content)

        def parse_raw_tx(self, tx_payload: str):
            self.logger.debug(f'parse raw tx: %s', tx_payload)
            try:
                b64_decode = base64.b64decode(tx_payload)
            except Exception as e:
                raise Exception('invalid base64 signedTx input.')

            tx = cbor.loads(b64_decode)
            tx_obj = utils.convert_raw_tx_to_obj(tx, {
                'txTime': datetime.utcnow(),
                'txOrdinal': None,
                'status': TX_PENDING_STATUS,
                'blockNum': None,
                'blockHash': None,
            })
            return tx_obj

        async def store_tx_as_pending(self, tx):
            self.logger.debug('store tx as pending: %s', tx)
            await self.db.store_tx(tx)

        async def validate_tx(self, tx_obj):
            try:
                await self.validate_tx_witnesses(
                    tx_obj['id'], 
                    tx_obj['inputs'], 
                    tx_obj['witnesses']
                )
                self.validate_destination_network(tx_obj['outputs'])

                return None
            except Exception as e:
                self.logger.exception(e)
                return str(e)

        async def validate_tx_witnesses(self, tx_id, inputs, witnesses):
            self.logger.debug(f'validate witnesses for tx: {tx_id}')
            if len(inputs) != len(witnesses):
              raise Exception(f'length of inputs: {len(inputs)} not equal length of witnesses: {len(witnesses)}')

            tx_hashes = list(set([inp['txId'] for inp in inputs]))
            full_outputs = await self.db.get_outputs_for_tx_hashes(tx_hashes)
            for inp, witness in zip(inputs, witnesses):
                input_type, input_tx_id, input_idx = inp
                witnessType, sign = witness
                if input_type != 0 or witnessType != 0:
                    self.logger.debug(f'ignore non-regular input/witness types: %s/%s', input_type, witnessType)

                tx_outputs = full_outputs.get(input_tx_id)
                if not tx_outputs:
                    raise Exception(f'No UTXO is found for tx {input_tx_id}! Maybe the blockchain is still syncing? If not, something is wrong.')

                input_address, input_amount = tx_outputs[input_idx]
                self.logger.debug('validate witness for input: %s.%s (%s coin from %s)', input_tx_id, input_idx, input_amount, input_address)
                address_root, addr_attr, address_type = self.deconstruct_address(input_address)
                if address_type != 0:
                    self.logger.debug('Unsupported address type: %s. skip witness validation for this input.', address_type)
                    return

                address_root_hex = address_root.toString('hex')
                expected_struct = [0, [0, sign[0]], addr_attr]
                encoded_struct = cbor.encodeCanonical(expected_struct)
                bytes_struct = bytes.fromhex(sha3_256(encoded_struct).digest())
                expected_root_hex = blake2b(bytes_struct, digest_size=28).hexdigest()
                if address_root_hex != expected_root_hex:
                    raise Exception('Witness does not match: %s != %s', address_root_hex, expected_root_hex)

        def validate_destination_network(self, outputs):
            self.logger.debug('validate output network.')
            for i, out in enumerate(outputs):
                address = out['address']
                self.logger.debug('validate network for %s', address)
                addr_attr = self.deconstruct_address(address)
                network_attr = addr_attr and addr_attr.get and addr_attr.get(2)
                network_magic = network_attr and network_attr.readInt32BE(1)
                if network_magic != self.expected_network_magic:
                    raise Exception('output %s network magic is %s, expected %s' % (i, network_magic, self.expected_network_magic))

        @staticmethod 
        def deconstruct_address(address: str):
            address_root, addr_attr, address_type = cbor.loads(
              cbor.loads(base58.b58decode(address))[0].value
            )

            return {
                'address_root': address_root, 
                'addr_attr': addr_attr, 
                'address_type': address_type
            }
