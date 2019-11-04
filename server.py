import asyncio
from db import DB
from lib.logger import get_logger
from models.http_bridge import HttpBridge
from models.genesis import Genesis
from models.scheduler import Scheduler
from routers import Routers
from tornado.web import Application
from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado.log import enable_pretty_logging
enable_pretty_logging()


logger = get_logger('server')

async def main():
    database = DB()
    http_bridge = HttpBridge()
    is_loaded = await database.is_genesis_loaded()
    if not is_loaded:
        logger.info('start to load genesis.')
        genesis = Genesis()
        genesis_file = await http_bridge.get_genesis(genesis.genesis_hash)
        if genesis_file.get('nonAvvmBalances'):
            utxos = genesis.non_avvm_balances_to_utxos(genesis_file['nonAvvmBalances'])
            await database.store_utxos(utxos)

        if genesis_file.get('avvmDistr'):
            utxos = genesis.avvm_distr_to_utxos(
                genesis_file['avvmDistr'], 
                genesis_file['protocolConsts']
            )
            await database.store_utxos(utxos)
        logger.info('genesis data is loaded.')
    else:
        logger.info('genesis has already loaded.')

    scheduler = Scheduler()
    await scheduler.start()

    logger.info('server is running.')


if __name__ == '__main__':

    # parser = argparse.ArgumentParser(description='cardano block chain data importer')
    define('port', type=int, default=9090, help='server listen port')
    # args = parser.parse_args()
    routers = Routers()

    app = Application(routers())
    app.listen(options.port)

    logger.info('server is listen on port: %d', options.port)

    loop = IOLoop.current()
    loop.run_sync(main)
    loop.start()
