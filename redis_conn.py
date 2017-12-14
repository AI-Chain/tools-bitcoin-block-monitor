# -*- encoding: utf-8 -*-

import os
import redis
import env_setup

host = os.environ.get("REDIS_HOST")
port = int(os.environ.get("REDIS_PORT"))
db = os.environ.get("REDIS_DB")

class RedisPool():

  __instance__ = None
  __pool__ = None
  __conn__ = None
  __cursor__ = None
  
  @staticmethod
  def getInstance():
    """
      get Redis connection
    """
    if RedisPool.__instance__ is None:

      RedisPool.__pool__ = redis.ConnectionPool(host = host, port = port, db = db)
      RedisPool.__instance__ = redis.Redis(connection_pool=RedisPool.__pool__)

    return RedisPool.__instance__

if __name__ == '__main__':
  # example
  redis_conn = RedisPool.getInstance()
  print redis_conn