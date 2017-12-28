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
utxo_item_inserted = 'utxo_item_inserted'
tx_inserted = 'tx_inserted'

switch_state = 'switch_state'

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


def add_utxo_items (tx):
  '''
    @param dict tx 
    @return string
  '''

  redis_conn = RedisPool.getInstance()

  mdb_conn = get_mongo_conn()
  btc_db = mdb_conn[bitcoin_utxo_db]

  for vout in tx['vout']:
    # money
    vout['value'] = float(vout['value'])

    if 'addresses' not in vout['scriptPubKey']:
      logger.info('[addresses] not found in vout->scriptPubKey')
      continue

    for addr in vout['scriptPubKey']['addresses']:
      # add new utxo data

      _id = build_id(tx['txid'], vout['n'], addr)

      item = redis_conn.hget(utxo_item_inserted, _id)

      if not item:

        try:
          data = {
            '_id': _id,
            'address': addr,
            'txid': tx['txid'],
            'vout_n': vout['n'],
            'amount': vout['value'], # satoshis = vout['value'] * 100000000
            'blockhash': tx['blockhash'],
            'confirmations': tx['confirmations'],
            'tx_type': 0, # 0: income, 1: expenditure
            'blocktime': tx['blocktime'],
            'block_header_time': tx['time']
          }

          btc_db.utxo_item.insert_one(data)
        except DuplicateKeyError, de: 
          pass
        # utxo_item_inserted
        redis_conn.hset(utxo_item_inserted, _id, '1')

  logger.info('[insert-new-items] txid: %s, blockhash: %s, vout_count: %s'% (tx['txid'], tx['blockhash'], len(tx['vout'])) )
  
  mdb_conn.close()


def get_save_tx (txid):
  '''
    Get and Save tx
  '''
  redis_conn = RedisPool.getInstance()
  mdb_conn = get_mongo_conn()
  btc_db = mdb_conn[bitcoin_utxo_db]

  # check tx inserted
  is_tx_inserted = redis_conn.hget(tx_inserted, txid)

  if not is_tx_inserted:
    rpc_conn = get_rpc_conn()

    tx = rpc_conn.getrawtransaction(txid, 1)
    if 'hex' in tx:
      del tx['hex']

    try:
      btc_db.tx.insert({
        "_id": txid,
        "data": tx
      })
    except DuplicateKeyError, de:
      pass
    mdb_conn.close()
    redis_conn.hset(tx_inserted, txid, '1')

    tx['is_inserted_before'] = False
    return tx

  else: 
    logger.info('[tx-inserted] txid: %s' % txid)
    tx = btc_db.tx.find_one({'_id': txid})
    mdb_conn.close()

    tx['data']['is_inserted_before'] = True
    return tx['data']


def save_tx (txid):
  '''
    Save Bitcoin translation data
    @param object block block information
    @param object tx translation information
  '''

  tx = get_save_tx(txid)
  
  # utxo inserted
  if tx['is_inserted_before']:
    logger.info('[utxo-is-inserted-before-vout] %s'%(txid) )
    return

  add_utxo_items(tx)

  mdb_conn = get_mongo_conn()
  btc_db = mdb_conn[bitcoin_utxo_db]

  # add utxo
  tx_in_txids = []
  tx_in_dict = {}
  for vin in tx['vin']:
    if not 'coinbase' in vin:
      if vin['txid'] not in tx_in_txids:
        tx_in_txids.append(vin['txid'])

  if len(tx_in_txids) > 0:
    rpc_conn = get_rpc_conn()
    for i in tx_in_txids:
      tx_in = get_save_tx(i)
      tx_in_dict[i] = tx_in

  # utxo item id array
  ids = []
  for vin in tx['vin']:
    # set translation coinbase flag

    if 'coinbase' in vin:
      btc_db.tx.update({'_id': tx['txid']}, {
        '$set': {
          'is_coinbase': 1,
          'coinbase': vin['coinbase']
        }
      })

    else:
      tx_in = tx_in_dict[vin['txid']]

      # new utxo
      if not tx_in['is_inserted_before']:
        add_utxo_items(tx_in)
      else:
        logger.info('[utxo-is-inserted-before-vin] %s'%(txid) )

      if 'addresses' in tx_in['vout'][vin['vout']]['scriptPubKey']:

        for vin_addr in tx_in['vout'][vin['vout']]['scriptPubKey']['addresses']:
          _id = build_id (tx_in['txid'], vin['vout'], vin_addr)
          ids.append(_id)

  mdb_conn.close()

  # update utxo trade type
  if ids:
    mdb_conn = get_mongo_conn()
    btc_db = mdb_conn[bitcoin_utxo_db]

    btc_db.utxo_item.update({'_id': {'$in': ids} }, {
      '$set': {
        'tx_type': 1,
        'next_txid': tx['txid']
      }
    })
    mdb_conn.close()
    logger.info('[utxo-tx-type-1] txid: %s'% (','.join(ids)))

if __name__ == '__main__': 

  while 1:
    txid = False
    try:
      redis_conn = RedisPool.getInstance()

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