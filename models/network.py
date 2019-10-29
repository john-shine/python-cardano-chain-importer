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
        self._network_name = config.get('defaultNetwork')
        network = get_network_config(self._network_name)
        self._network_url = urljoin(network['bridgeUrl'], self._network_name) + '/'
        self._genesis_hash = network['genesis']
        self._start_time = network['startTime']
        self._network_magic = network['networkMagic']

    @property
    def network_name(self):
        return self._network_name

    @property
    def start_time(self):
        return self._start_time

    @property
    def genesis_hash(self):
        return self._genesis_hash

    @property
    def network_url(self):
        return self._network_url

    @property
    def network_magic(self):
      return self._network_magic
