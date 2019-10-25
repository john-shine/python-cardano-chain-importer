import asyncio
from db import DB
from lib.logger import get_logger
from models.http_bridge import HttpBridge
from models.genesis import Genesis
from models.scheduler import Scheduler
from routers import Routers
from tornado.web import Application
from tornado.ioloop import IOLoop
import argparse


async def main(loop):
    logger = get_logger('server')
    db = DB()
    http_bridge = HttpBridge()
    is_loaded = db.is_genesis_loaded()
    if not is_loaded:
        logger.info('start to load genesis.')
        genesis = Genesis()
        genesis_file = http_bridge.get_genesis(genesis.genesis_hash)
        if genesis_file.get('nonAvvmBalances'):
            utxos = genesis.non_avvm_balances_to_utxos(genesis_file['nonAvvmBalances'])
            await db.store_utxos(utxos)

        if genesis_file.get('avvmDistr'):
            utxos = genesis.avvm_distr_to_utxos(
                genesis_file['avvmDistr'], 
                genesis_file['protocolConsts']
            )
            await db.store_utxos(utxos)
        logger.info('genesis data is loaded.')
    else:
        logger.info('genesis has already loaded.')

    scheduler = Scheduler()
    asyncio.add_task(scheduler.start())

    logger.info('server is running.')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='cardano block chain data importer')
    parser.add_argument('--port', type=int, default=9090, help='server listen port')
    args = parser.parse_args()

    app = Application()
    app.listen(args.port)

    loop = IOLoop.current()
    loop.start()
