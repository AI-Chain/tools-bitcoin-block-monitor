# -*- encoding: utf8 -*-

'''
  Bitcoin block monitor
'''

import time
import env_setup
from redis_conn import RedisPool
from bitcoin_rpc_conn import get_rpc_conn
from log_helper import get_logger

logger = get_logger(__file__)

polling_size = 100
btc_scan = 'btc_scan'
btc_scan_lastest_height = 'btc_scan_lastest_height'
txid_list = 'txid_list'

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

def blocks_utxo_scan (height_start, height_end, best_block_height = None):
  '''
    block scan
    @paran int height_start
    @paran int height_end
  '''
  time_butxo_scan_start = time.time()

  rpc_conn = get_rpc_conn()

  commands = [ [ "getblockhash", h] for h in range(height_start, height_end) ]
  block_hashes = rpc_conn.batch_(commands)
  blocks = rpc_conn.batch_([ [ "getblock", h ] for h in block_hashes ])

  time_fetch_blocks_end = time.time()

  logger.info('[time-fetch-blocks]:  %s'%( time_fetch_blocks_end - time_butxo_scan_start ) )

  # polling  
  for block in blocks:
    for txid in block['tx']:

      time_push_utxo_start = time.time()

      redis_conn = RedisPool.getInstance()
      redis_conn.rpush(txid_list, txid)

      time_push_utxo_end = time.time()

      logger.info('[time-push-utxo]:  %s'%( time_push_utxo_end - time_push_utxo_start ) )

    time_set_last_height_start = time.time()
    redis_conn = RedisPool.getInstance()
    redis_conn.hset(btc_scan, btc_scan_lastest_height, block['height'])
    logger.info('[block-scan] block height: %s / %s'%(block['height'], best_block_height))
    time_set_last_height_end = time.time()
    logger.info('[time-set-last-height]:  %s'%( time_set_last_height_end - time_set_last_height_start ) )


  time_butxo_scan_end = time.time()

def block_monitor ():
  '''
    block database sacning
    @paran int scan_start_step 
  '''
  time_block_scan_start = time.time()

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

    blocks_utxo_scan(height_start, height_end, best_block_height)

    time_block_scan_end = time.time()

if __name__ == '__main__': 

  try:
    block_monitor()
  except Exception, e:
    logger.error('[block-scan-error] %s'%( str(e) ) )

  # while 1:
  #   block_monitor()
  #   time.sleep(0.5)