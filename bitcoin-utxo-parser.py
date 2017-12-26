# -*- encoding: utf8 -*-

'''
  Bitcoin translation data init
'''

import time
import traceback
import json
import decimal
import xxhash
from bitcoinrpc.authproxy import JSONRPCException
from pymongo.errors import DuplicateKeyError
import env_setup
from redis_conn import RedisPool
from mongo_conn import get_mongo_conn
from bitcoin_rpc_conn import get_rpc_conn
from log_helper import get_logger

logger = get_logger(__file__)

bitcoin_utxo_db = 'btc_db'
txid_list = 'txid_list'
txid_inserted = 'txid_inserted'

def decimal_default (obj):
  '''
    Decimal in json dumps
  '''
  if isinstance(obj, decimal.Decimal):
    return float(obj)
  raise TypeError


def build_id (txid, vout_n, addr):
  '''
    build unique id (txid-vout[index]-addr)
    @param string txid UTXO id 
    @param int vout_n 
    @param string addr
  '''
  return xxhash.xxh64('%s-%s-%s'%(txid, vout_n, addr)).hexdigest()


def add_utxos (tx):
  '''
    @param dict tx 
    @return string
  '''
  for vout in tx['vout']:
    # money
    vout['value'] = float(vout['value'])

    if 'addresses' not in vout['scriptPubKey']:
      logger.info('[addresses] not found in vout->scriptPubKey')
      continue

    mdb_conn = get_mongo_conn()
    btc_db = mdb_conn[bitcoin_utxo_db]

    redis_conn = RedisPool.getInstance()

    for addr in vout['scriptPubKey']['addresses']:
      # add new utxo data

      _id = build_id(tx['txid'], vout['n'], addr)

      item = redis_conn.hget(txid_inserted, _id)

      if not item:

        data = {
          '_id': _id,
          'address': addr,
          'txid': tx['txid'],
          'vout_n': vout['n'],
          'amount': vout['value'], # satoshis = vout['value'] * 100000000
          'blockhash': tx['blockhash'],
          'confirmations': tx['confirmations'],
          'is_coinbase': 0,
          'tx_type': 0, # 0: income, 1: expenditure
          'blocktime': tx['blocktime'],
          'block_header_time': tx['time']
        }

        btc_db.utxo.insert_one(data).inserted_id
        logger.info('[insert-utxo] inserted_id: %s, txid: %s, blockhash: %s, vout_n: %s, amount: %s'% (_id, tx['txid'], tx['blockhash'], vout['n'], vout['value']))

        # txid_inserted
        redis_conn.hset(txid_inserted, _id, '1')
      else :
        logger.info('[insert-utxo-dupulicate] inserted_id: %s, txid: %s, blockhash: %s, vout_n: %s, amount: %s'% (_id, tx['txid'], tx['blockhash'], vout['n'], vout['value']))


      return _id


def save_tx (tx):
  '''
    Save Bitcoin translation data
    @param object block block information
    @param object tx translation information
  '''
  
  rpc_conn = get_rpc_conn()
  tx = rpc_conn.getrawtransaction(txid, 1)
  add_utxos(tx)

  mdb_conn = get_mongo_conn()
  btc_db = mdb_conn[bitcoin_utxo_db]

  ids = []
  for vin in tx['vin']:
    # set translation coinbase flag

    if 'coinbase' in vin:

      btc_db.utxo.update({'txid': tx['txid']}, {
        '$set': {
          'is_coinbase': 1,
          'coinbase': vin['coinbase']
        }
      })
    else:
      rpc_conn = get_rpc_conn()
      time_get_tx_in_start = time.time()
      tx_in = rpc_conn.getrawtransaction(vin['txid'], 1)
      time_get_tx_in_end = time.time()
      # logger.info('===========>add tx_in: %s, %s, %s' % (time_get_tx_in_end-time_get_tx_in_start, vin['txid'], json.dumps(tx_in, default=decimal_default) ) )
      add_utxos(tx_in)

      if 'addresses' in tx_in['vout'][vin['vout']]['scriptPubKey']:

        for vin_addr in tx_in['vout'][vin['vout']]['scriptPubKey']['addresses']:
          _id = build_id (tx_in['txid'], vin['vout'], vin_addr)
          ids.append(_id)

  # update utxo trade type
  if ids:
    mdb_conn = get_mongo_conn()
    btc_db = mdb_conn[bitcoin_utxo_db]

    btc_db.utxo.update({'_id': {'$in': ids}}, {
      '$set': {
        'tx_type': 1,
        'next_txid': tx['txid']
      }
    })

    logger.info('[utxo-tx-type-1] txid: %s'% (','.join(ids)))

if __name__ == '__main__': 
  while 1:
    txid = False
    try:
      redis_conn = RedisPool.getInstance()
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

