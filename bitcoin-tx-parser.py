# -*- encoding: utf8 -*-

'''
  Bitcoin parse/save Translation Data 
'''

import os
import time
import traceback
import json
import decimal
from pymongo.errors import DuplicateKeyError
import env_setup

from redis_conn import RedisPool
from mongo_conn import get_mongo_conn
from bitcoin_rpc_conn import get_rpc_conn
from log_helper import get_logger

logger = get_logger(__file__)

mongo_host = os.environ.get("MONGO_HOST_TX")
mongo_port = os.environ.get("MONGO_PORT_TX")

bitcoin_utxo_db = 'btc_db'
txid_list = 'txid_list'
tx_item_list = 'tx_item_list'
switch_state = 'switch_state'

def decimal_default (obj):
  '''
    Decimal in json dumps
  '''
  if isinstance(obj, decimal.Decimal):
    return float(obj)
  raise TypeError


def save_tx (txid):
  '''
    Save Bitcoin translation data
    @param object block block information
    @param object tx translation information
  '''

  rpc_conn = get_rpc_conn()
  tx = rpc_conn.getrawtransaction(txid, 1)
  if 'hex' in tx:
    del tx['hex']

  if 'vin' in tx :
    for i in range(0, len(tx['vin'])):
      if 'scriptSig' in tx['vin'][i]:
        del tx['vin'][i]['scriptSig']

  if 'vout' in tx :
    for i in range(0, len(tx['vout'])):
      if 'scriptSig' in tx['vout'][i]:
        del tx['vout'][i]['scriptSig']
    

  is_coinbase = 0
  coinbase_vin = ''

  for vout in tx['vout']:

    if 'addresses' not in vout['scriptPubKey']:
      logger.info('[addresses] not found in vout->scriptPubKey')
      continue

    redis_conn = RedisPool.getConn()
    for addr in vout['scriptPubKey']['addresses']:
      redis_conn.rpush(tx_item_list, 'out,%s,%s,%s'% (tx['txid'], vout['n'], addr) )

  for vin in tx['vin']:
    # set translation coinbase flag
    if 'coinbase' in vin:
      is_coinbase = 1
      coinbase_vin = vin['coinbase']

    else:
      redis_conn.rpush(tx_item_in_list, 'in,%s,%s,%s'% (txid, vin['txid'], vin['vout']) )

  mdb_conn = get_mongo_conn(mongo_host, mongo_port)
  btc_db = mdb_conn[bitcoin_utxo_db]

  try:
    tx['is_coinbase'] = is_coinbase
    tx['coinbase_vin'] = coinbase_vin
    btc_db.tx.insert({
      "_id": txid,
      "data": tx
    })
  except DuplicateKeyError, de:
    mdb_conn.close()

if __name__ == '__main__': 

  while 1:
    txid = False
    try:
      redis_conn = RedisPool.getConn()

      sw_state = redis_conn.hget(switch_state, 'state')
      if sw_state == 'off':
        logger.info('[sw_state off]')
        time.sleep(10)
        continue

      if sw_state == 'exit':
        logger.info('[sw_state exit]')
        break

      txid = redis_conn.lpop(txid_list)
      if txid:
        logger.info('[utxo-txid-start-parse] txid: %s'% (txid))
        save_tx(txid)
      else:
        time.sleep(0.5)
    except Exception, e:
      if txid:
        redis_conn.lpush(txid_list, txid)

      logger.error(traceback.format_exc())