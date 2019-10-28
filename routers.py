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
from lib.logger import get_logger
from lib import utils
from constants.tx import TX_STATUS
from models.http_bridge import HttpBridge
from db import DB
from cbor import cbor
from datetime import datetime
import json
import base58
from tornado.web import RequestHandler
from models.network import Network


class Routers:

    def __init__(self):
        self.logger = get_logger('routers')
        self.data_provider = HttpBridge()
        self.db = DB()
        self.expected_network_magic = Network.network_magic

    def __call__(self):
        return [
            (r'/api/txs/signed', self.SignHandler)
        ]

    class SignHandler(RequestHandler):

        def set_default_headers(self):
            self.set_header("Content-Type", 'application/json')

        def fail(self, message):
            return self.write(json.dumps({'success': False, 'message': message}))

        def success(self):
            return self.write(json.dumps({'success': True, 'message': 'OK'}))

        async def post(self):
            try:
                body = json.loads(self.request.body)
            except json.decoder.JSONDecodeError:
                return self.fail('invalid request')

            if not isinstance(body, dict):
                return self.fail('invalid request')

            signed_tx = body.get('signedTx')
            if not signed_tx:
                return self.fail('signedTx is empty')

            tx_obj = self.parse_raw_tx(signed_tx)
            local_validation_error = await self.validate_tx(tx_obj)
            if (local_validation_error):
              self.logger.error('Local tx validation failed: ${local_validation_error}')
              self.logger.info('Proceeding to send tx to network for double-check')

            bridge_resp = await self.data_provider.post_signed_tx(req.rawBody)
            self.logger.debug('TxController.index called', req.params, bridge_resp.status, '(${bridge_resp.statusText})', bridge_resp.data)
            try:
                if bridge_resp.status == 200:
                    # store tx as pending
                    await self.store_tx_as_pending(tx_obj)
                    if local_validation_error:
                        # Network success but locally we failed validation - log local
                        self.logger.warn('Local validation error, but network send succeeded!')
            except Exception as err:
                self.logger.error('Failed to store tx as pending!', err);
                raise Exception('Internal DB fail in the importer!')

            statusText = None
            status = None
            resp_body = None
            if local_validation_error and bridge_resp.status != 200:
                # We have local validation error and network failed too
                # We send specific local response with network response attached
                status = 400
                statusText = 'Transaction failed local validation (Network status: ${bridge_resp.statusText})'
                resp_body = 'Transaction validation error: ${local_validation_error} (Network response: ${bridge_resp.data})'
            else:
                # Locally we have no validation errors - proxy the network response
                status, statusText = bridge_resp
                resp_body = bridge_resp.data

            resp.status(status)
            # eslint-disable-next-line no-param-reassign
            resp.statusText = statusText
            resp.send(resp_body)
            next()

        def parse_raw_tx(self, tx_payload: str):
            self.logger.debug('txs.parse_raw_tx ${tx_payload}')
            now = datetime.utcnow()
            tx = cbor.loads(bytes.fromhex(tx_payload, 'base64'))
            tx_obj = utils.raw_tx_to_obj(tx, {
                'txTime': now,
                'txOrdinal': None,
                'status': TX_STATUS.TX_PENDING_STATUS,
                'blockNum': None,
                'blockHash': None,
            })
            return tx_obj

        async def store_tx_as_pending(self, tx):
            self.logger.debug('txs.storeTxAsPending ${JSON.strify(tx)}')
            await self.db.store_tx(tx)

        async def validate_tx(self, tx_obj):
            try:
              await self.validate_tx_witnesses(tx_obj)
              self.validate_destination_network(tx_obj)
              # TODO: more validation
              return None
            except Exception as e:
              raise

        async def validate_tx_witnesses(self, id, inputs, witnesses):
            inpLen = len(inputs)
            witLen = len(witnesses)
            self.logger.debug(f'Validating witnesses for tx: ${id} (inputs: ${inpLen})')
            if inpLen != witLen:
              raise Exception(f'Number of inputs (${inpLen}) != the number of witnesses (${witLen})')

            txHashes = set([inp['txId'] for inp in inputs])
            fullOutputs = await self.db.get_outputs_for_tx_hashes(txHashes)
            for inp, witness in zip(inputs, witnesses):
                  inputType, inputTxId, inputIdx = inp
                  witnessType, sign = witness
                  if inputType != 0 or witnessType != 0:
                      self.logger.debug(f'Ignoring non-regular input/witness types: ${json.dumps({ inputType, witnessType })}')

                  txOutputs = fullOutputs[inputTxId];
                  if not txOutputs:
                      raise Exception('No UTxO is found for tx ${inputTxId}! Maybe the blockchain is still syncing? If not - something is wrong.')

                  inputAddress, inputAmount = txOutputs[inputIdx]
                  self.logger.debug(f'Validating witness for input: ${inputTxId}.${inputIdx} (${inputAmount} coin from ${inputAddress})')
                  addressRoot, addrAttr, addressType = self.deconstruct_address(inputAddress)
                  if addressType != 0:
                    self.logger.debug('Unsupported address type: ${addressType}. Skipping witness validation for this input.')
                    return

                  addressRootHex = addressRoot.toString('hex')
                  expectedStruct = [0, [0, sign[0]], addrAttr]
                  encodedStruct = bytes.fromhex(sha3_256.update(
                    cbor.encodeCanonical(expectedStruct)).digest())
                  expectedRootHex = blake.blake2bHex(encodedStruct, None, 28)
                  if addressRootHex != expectedRootHex:
                      raise Exception('Witness does not match! ${JSON.strify({ addressRootHex, expectedRoot: expectedRootHex })}')

        def validate_destination_network(self, outputs):
            self.logger.debug('Validating output network (outputs: ${outputs.length})')
            for i, out in enumerate(outputs):
              address = out['address']
              self.logger.debug(f'Validating network for ${address}')
              addrAttr = self.deconstruct_address(address)
              network_attr = addrAttr and addrAttr.get and addrAttr.get(2)
              network_magic = network_attr and network_attr.readInt32BE(1)
              if network_magic != self.expected_network_magic:
                raise Exception(f'Output #${i} network magic is ${network_magic}, expected ${self.expected_network_magic}')

        @staticmethod 
        def deconstruct_address(cls, address: str):
            [addressRoot, addrAttr, addressType] = cbor.loads(
              cbor.loads(base58.b58decode(address))[0].value
            )
            return { addressRoot, addrAttr, addressType }
