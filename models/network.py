# import { helpers } from 'inversify-vanillajs-helpers'
# import config from 'config'

# import urljoin from 'url-join'

# import utils from '../utils'
# import type { NetworkConfig } from '../interfaces/network-config'
from config import config
from urllib.parse import urljoin
from lib.utils import get_network_config


class Network:

    def __init__(self):
        network = config.get('network')
        self.network_url = urljoin(config['bridgeUrl'], network['name']) + '/'
        self.genesis_hash = network['genesis']
        self.start_time = network['startTime']
        self.network_magic = network['networkMagic']
