# -*- encoding: utf-8 -*-

import os
import redis
from bitcoinrpc.authproxy import AuthServiceProxy
import env_setup

BITCOIN_RPC_HOST = os.environ.get("BITCOIN_RPC_HOST")
BITCOIN_RPC_PORT = os.environ.get("BITCOIN_RPC_PORT")
BITCOIN_RPC_USER = os.environ.get("BITCOIN_RPC_USER")
BITCOIN_RPC_PWD = os.environ.get("BITCOIN_RPC_PWD")

def get_rpc_conn ():
  '''
    get bitcoin rpc connection
  '''
  return AuthServiceProxy("http://%s:%s@%s:%s"%(BITCOIN_RPC_USER, BITCOIN_RPC_PWD, BITCOIN_RPC_HOST, BITCOIN_RPC_PORT))