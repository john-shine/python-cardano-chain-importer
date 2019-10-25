# import urljoin from 'url-join'
# import axios from 'axios'

# import { helpers } from 'inversify-vanillajs-helpers'

# import { RawDataProvider, RawDataParser } from '../../interfaces'
# import SERVICE_IDENTIFIER from '../../constants/identifiers'
# import type { NetworkConfig } from '../../interfaces'

from tornado.httpclient import AsyncHTTPClient
from urllib.parse import urljoin
from models.network import Network


class HttpBridge:

    def __init__(self, parser):
        self.network_url = Network().network_url
        self.parser = parser
        self.client = AsyncHTTPClient()

    async def get(self, path: str, params={}):
        endpoint_url = urljoin(self.network_url, path)
        try:
            resp = await self.client.fetch(endpoint_url, method='GET')
            return resp
        except Exception as e:
            if e.code == 'ECONNREFUSED':
                error = Exception('cardano-http-bridge is not accessible (ECONNREFUSED)')
                error.code = 'NODE_INACCESSIBLE'
                raise error

            raise

    async def post(self, path: str, data: str):
        endpoint_url = urljoin(self.network_url, path)
        try:
            resp = await self.client.fetch(endpoint_url, method='POST', body=data)
        except Exception as e:
            raise

        return resp

    async def get_json(self, path: str):
        resp = await self.get(path)
        try:
            resp = json.loads(resp)
            return resp
        except Exception as e:
            raise Exception('invalid json resp: %s' % resp)

    async def get_tip(self):
        resp = await self.get('/tip')
        return resp

    async def post_signed_tx(self, payload: str):
        resp = await self.post('txs/signed', payload)
        return resp

    async def get_epoch(self, id: int):
        resp = await self.get(f'/epoch/${id}')
        return resp['data']

    async def get_block(self, id: str): 
        resp = await self.get(f'/block/${id}')
        return resp['data']

    async def get_genesis(self, hash: str): 
        resp = await self.get_json(f'/genesis/${hash}')
        return resp['data']

    async def get_status(self): 
        resp = await self.get_json('/status')
        return resp['data']

    async def get_block_by_height(self, height: int):
        resp = await self.get(f'/height/${height}')
        return self.parser.parse_block(resp['data'])

    async def get_parsed_epoch_by_id(self, epoch_id: int, is_omit_ebb=False):
        resp = await self.get(f'/epoch/${epoch_id}')
        blocks_iterator = self.parser.parse_epoch(resp['data'], {'omitEbb': is_omit_ebb})

        return blocks_iterator
