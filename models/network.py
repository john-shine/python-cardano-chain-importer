from config import config
from urllib.parse import urljoin


class Network:

    def __init__(self):
        network = config.get('network')
        self.network_url = urljoin(config['bridgeUrl'], network['name']) + '/'
        self.genesis_hash = network['genesis']
        self.start_time = network['startTime']
        self.network_magic = network['networkMagic']
