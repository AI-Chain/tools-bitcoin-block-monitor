# -*- encoding: utf8 -*-

import sys
from redis_conn import RedisPool

bitcoin_monitor_state = 'bitcoin_monitor_state'

if __name__ == '__main__':

  if len (sys.argv) >= 2 and sys.argv[1] in ['off', 'on', 'exit']:
    redis_conn = RedisPool.getConn()
    redis_conn.hset('switch_state', 'state', sys.argv[1])