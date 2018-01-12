# -*- encoding: utf-8 -*-

import os
import redis
from hash_ring import HashRing
import env_setup

host = os.environ.get("REDIS_HOST_MONITOR")
port = int(os.environ.get("REDIS_PORT_MONITOR"))
db = os.environ.get("REDIS_DB_MONITOR")

redis_txs_services = os.environ.get("REDIS_TX_SERVERS").split(',')

txs_port = os.environ.get("REDIS_PORT_TX_SERVER")
txs_db = os.environ.get("REDIS_DB_TX_SERVER")

redis_ring = HashRing(redis_txs_services)

class RedisPool():

  __instance__ = None
  __pool__ = None
  __conn__ = None
  __cursor__ = None
  
  @staticmethod
  def getConn():
    """
      get Redis connection
    """
    if RedisPool.__instance__ is None:

      RedisPool.__pool__ = redis.ConnectionPool(host = host, port = port, db = db)
      RedisPool.__instance__ = redis.Redis(connection_pool=RedisPool.__pool__, socket_connect_timeout=2)

    return RedisPool.__instance__

  @staticmethod
  def getTxConn(k = ''):
    host = redis_ring.get_node(k)
    return redis.StrictRedis(host=host, port=txs_port, db=txs_port)

if __name__ == '__main__':

  print RedisPool.getTxConn('12345')

#   # example
  # redis_conn = RedisPool.getConn()
