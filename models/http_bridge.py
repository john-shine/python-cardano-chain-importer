import json
from urllib.parse import urljoin
from models.network import Network
from models.parser import Parser
from lib.logger import get_logger
from tornado.httpclient import AsyncHTTPClient, HTTPClientError


class HttpBridge:

    def __init__(self):
        self.network_url = Network().network_url
        self.parser = Parser()
        self.client = AsyncHTTPClient()
        self.logger = get_logger('http-bridge')

    async def get(self, path: str, params={}):
        endpoint_url = urljoin(self.network_url, path)
        self.logger.info('GET %s params: %s', endpoint_url, params)
        try:
            resp = await self.client.fetch(endpoint_url, method='GET')
            return resp
        except HTTPClientError as e:
            if e.code == 'ECONNREFUSED':
                raise Exception('cardano-http-bridge is not accessible (ECONNREFUSED)')

            raise

    async def post(self, path: str, data: str):
        endpoint_url = urljoin(self.network_url, path)
        self.logger.info('POST %s data: %s', endpoint_url, data)
        try:
            resp = await self.client.fetch(endpoint_url, method='POST', body=data)
            return resp
        except HTTPClientError as e:
            if e.code == 'ECONNREFUSED':
                raise Exception('cardano-http-bridge is not accessible (ECONNREFUSED)')

            raise

    async def get_json(self, path: str):
        resp = await self.get(path)
        try:
            resp = json.loads(resp.body)
            return resp
        except Exception as e:
            raise Exception('invalid json resp: %s' % str(resp.body)[:100])

    async def get_tip(self):
        resp = await self.get_json('tip')
        return resp

    async def post_signed_tx(self, payload: str):
        resp = await self.post('txs/signed', payload)
        return resp

    async def get_epoch(self, id: int):
        resp = await self.get_json(f'epoch/{id}')
        return resp

    async def get_block(self, id: str): 
        resp = await self.get_json(f'block/{id}')
        return resp

    async def get_genesis(self, hash: str): 
        return await self.get_json(f'genesis/{hash}')

    async def get_status(self): 
        resp = await self.get_json('status')
        return resp

    async def get_block_by_height(self, height: int):
        resp = await self.get(f'height/{height}')
        return self.parser.parse_block(resp.body)

    async def get_parsed_epoch_by_id(self, epoch_id: int, is_omit_ebb=False):
        resp = await self.get(f'epoch/{epoch_id}')
        blocks_iterator = self.parser.parse_epoch(resp.body, {'omitEbb': is_omit_ebb})

        return blocks_iterator
