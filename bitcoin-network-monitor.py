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

polling_size = 100
bitcoin_utxo_db = 'btc_db'
btc_scan_lastest_height = 'btc_scan_lastest_height'
btc_scan = 'btc_scan'

def decimal_default (obj):
  '''
    Decimal in json dumps
  '''
  if isinstance(obj, decimal.Decimal):
    return float(obj)
  raise TypeError


def _get_polling_step (start, end):
  '''
    get polling step
  '''
  start = int(start)
  end = int(end)

  total_steps = (end + polling_size - 1)  / polling_size

  start_step = start / polling_size

  return [start_step, total_steps]


def get_best_block_height ():
  '''
    get the lastest block height of the bitcon network
  '''

  rpc_conn = get_rpc_conn()
  best_block_hash = rpc_conn.getbestblockhash()
  best_block = rpc_conn.getblock(best_block_hash)
  return best_block['height']


def save_tx (rpc_conn, block, tx):
  '''
    Save Bitcoin translation data
    @param object rpc_conn: Bitcoin-core API RPC rpc_conn
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

  def _save_utxo (btc_db, block, tx):
    '''
      Save the translation information to the mongodb
      @param object btc_db the DB name
      @param object block block information
      @param object tx translation information
    '''

    for vout in tx['vout']:
      # money
      vout['value'] = float(vout['value'])

      if 'addresses' not in vout['scriptPubKey']:
        continue

      for addr in vout['scriptPubKey']['addresses']:

        _id = _build_id(tx['txid'], vout['n'], addr)
        data = {
          '_id': _id,
          'address': addr,
          'txid': tx['txid'],
          'vout_n': vout['n'],
          'scriptPubKey_hex': vout['scriptPubKey']['hex'],
          'amount': vout['value'], # satoshis = vout['value'] * 100000000

          'height': block['height'],
          'confirmations': tx['confirmations'],
          'is_coinbase': 0,
          'tx_type': 0 # 0: income, 1: expenditure
        }

        try:
          inserted_id = btc_db.utxo.insert_one(data).inserted_id
        except DuplicateKeyError, e:
          # print str(e)
          pass

  mdb_conn = get_mongo_conn()
  btc_db = mdb_conn[bitcoin_utxo_db]

  _save_utxo (btc_db, block, tx)

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
      tx_in = rpc_conn.getrawtransaction(vin['txid'], 1)

      if 'addresses' in tx_in['vout'][vin['vout']]['scriptPubKey']:
        for vin_addr in tx_in['vout'][vin['vout']]['scriptPubKey']['addresses']:
          
          _id = _build_id (tx_in['txid'], vin['vout'], vin_addr)

          btc_db.utxo.update({'_id': _id}, {
            '$set': {
              'tx_type': 1,
              'next_txid': tx['txid']
            }
          })

          # print '------------------------------------------>_id: %s, txid: %s, vout: %s, addr: %s, json: %s' % ( _id, tx_in['txid'], vin['vout'], vin_addr, json.dumps(tx, default=decimal_default) ) , '<------------------------------------------'


def blocks_utxo_scan (rpc_conn, height_start, height_end, best_block_height = None):
  '''
    block scan
    @paran object rpc_conn
    @paran int height_start
    @paran int height_end
  '''

  commands = [ [ "getblockhash", h] for h in range(height_start, height_end) ]
  block_hashes = rpc_conn.batch_(commands)
  blocks = rpc_conn.batch_([ [ "getblock", h ] for h in block_hashes ])

  # polling  
  for block in blocks:
    for txid in block['tx']:
      try:
        tx = rpc_conn.getrawtransaction(txid, 1)
        save_tx(rpc_conn, block, tx)
        
      except JSONRPCException, e:         
        logger.error('%s, {block_hash}: %s, {txid}: %s'% (str(e), block['hash'], txid) )

    redis_conn = RedisPool.getInstance()
    redis_conn.hset(btc_scan, btc_scan_lastest_height, block['height'])
    logger.info('[block-scan] block height: %s / %s'%(block['height'], best_block_height))

def block_monitor ():
  '''
    block database sacning
    @paran int scan_start_step 
  '''

  redis_conn = RedisPool.getInstance()
  btc_local_newest_height = redis_conn.hget(btc_scan, btc_scan_lastest_height)

  if not btc_local_newest_height:
    btc_local_newest_height = 0
  else: 
    btc_local_newest_height = int(btc_local_newest_height)

  best_block_height = get_best_block_height()

  if btc_local_newest_height == best_block_height:
    logger.info('[block-scan] btc_local_newest_height: %s / %s'%(btc_local_newest_height, best_block_height) )
    return

  if btc_local_newest_height > best_block_height:
    btc_local_newest_height = best_block_height - 1

  steps_start, steps_end = _get_polling_step(btc_local_newest_height, best_block_height)

  for scan_start_step in range(steps_start, steps_end):

    # [height_start, height_end)
    height_start = scan_start_step * polling_size
    height_end = height_start + polling_size

    if height_start < btc_local_newest_height:
      height_start = btc_local_newest_height

    if height_end > best_block_height:
      height_end = best_block_height + 1

    logger.info('[block-scan] step: %s / [%s, %s), block: %s / %s'%(scan_start_step, steps_start, steps_end, height_start, height_end) )

    rpc_conn = get_rpc_conn()
    blocks_utxo_scan(rpc_conn, height_start, height_end, best_block_height)

if __name__ == '__main__': 

  block_monitor()

  # while 1:
  #   block_monitor()
  #   time.sleep(0.5)