config = {
  "cardanoBridge": {
    "baseUrl": "http://localhost:8082",
    "template": "testnet2"
  },
  "server": {
    "port": 8080,
    "logLevel": "debug",
    "logRequests": "true",
    "apiConfig": {
      "addressesRequestLimit": 50,
      "txsHashesRequestLimit": 150,
      "txHistoryResponseLimit": 20,
      "txHistoryV2ResponseLimit": 50,
      "minimumTimeImporterHealthCheck": 90000
    }
  },
  "checkTipSeconds": 15,
  "rollbackBlocksCount": 25,
  "defaultNetwork": "mainnet",
  "defaultBridgeUrl": "http://localhost:8082",
  "networks": {
    "testnet2": {
      "genesis": "96fceff972c2c06bd3bb5243c39215333be6d56aaf4823073dca31afe5038471",
      "startTime": 1563999616,
      "networkMagic": 1097911063
    },
    "staging": {
      "genesis": "c6a004d3d178f600cd8caa10abbebe1549bef878f0665aea2903472d5abf7323",
      "startTime": 1506450213
    },
    "mainnet": {
      "genesis": "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb",
      "startTime": 1506203091
    }
  },
  "db": {
    "user": "",
    "host": "",
    "database": "",
    "password": "",
    "port": 5432,
    "timeout": 5
  }
}
