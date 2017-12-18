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

def decimal_default (obj):
  '''
    Decimal in json dumps
  '''
  if isinstance(obj, decimal.Decimal):
    return float(obj)
  raise TypeError


def save_tx (tx):
  '''
    Save Bitcoin translation data
    @param object block block information
    @param object tx translation information
  '''

  def _build_id (txid, vout_n, addr):
    '''
      build unique id (txid-vout[index]-addr)
      @param string txid UTXO id 
      @param int vout_n 
      @param string addr
    '''
    return xxhash.xxh64('%s-%s-%s'%(txid, vout_n, addr)).hexdigest()

  def _save_utxo (btc_db, txid):
    '''
      Save the translation information to the mongodb
      @param object btc_db the DB name
      @param string txid translation id
    '''

  #   for vout in tx['vout']:
  #     # money
  #     vout['value'] = float(vout['value'])

  #     if 'addresses' not in vout['scriptPubKey']:
  #       continue

  #     for addr in vout['scriptPubKey']['addresses']:

  #       _id = _build_id(tx['txid'], vout['n'], addr)
  #       data = {
  #         '_id': _id,
  #         'address': addr,
  #         'txid': tx['txid'],
  #         'vout_n': vout['n'],
  #         'scriptPubKey_hex': vout['scriptPubKey']['hex'],
  #         'amount': vout['value'], # satoshis = vout['value'] * 100000000

  #         'confirmations': tx['confirmations'],
  #         'is_coinbase': 0,
  #         'tx_type': 0 # 0: income, 1: expenditure
  #       }

  #       try:
  #         inserted_id = btc_db.utxo.insert_one(data).inserted_id
  #       except DuplicateKeyError, e:
  #         # print str(e)
  #         pass


  rpc_conn = get_rpc_conn()

  tx = rpc_conn.getrawtransaction(txid, 1)
  print tx
  # mdb_conn = get_mongo_conn()
  # btc_db = mdb_conn[bitcoin_utxo_db]

  # _save_utxo (btc_db, tx)

  # for vin in tx['vin']:
  #   # set translation coinbase flag
  #   if 'coinbase' in vin:
  #     btc_db.utxo.update({'txid': tx['txid']}, {
  #       '$set': {
  #         'is_coinbase': 1,
  #         'coinbase': vin['coinbase']
  #       }
  #     })

  #   else:
  #     tx_in = rpc_conn.getrawtransaction(vin['txid'], 1)

  #     if 'addresses' in tx_in['vout'][vin['vout']]['scriptPubKey']:
  #       for vin_addr in tx_in['vout'][vin['vout']]['scriptPubKey']['addresses']:
          
  #         _id = _build_id (tx_in['txid'], vin['vout'], vin_addr)

  #         btc_db.utxo.update({'_id': _id}, {
  #           '$set': {
  #             'tx_type': 1,
  #             'next_txid': tx['txid']
  #           }
  #         })

if __name__ == '__main__': 

  redis_conn = RedisPool.getInstance()
  txid = redis_conn.lpop(txid_list)
  save_tx(txid)