import time
import os
import redis
import env_setup

host = '172.16.133.83'
port = 6379
db = '1'

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
      RedisPool.__instance__ = redis.Redis(connection_pool=RedisPool.__pool__, socket_connect_timeout=2)

    return RedisPool.__instance__

if __name__ == '__main__':
  
  sec = 60
  arr = []
  index = 0
  total = 0
  while 1:
    if index == sec: break

    redis_conn = RedisPool.getInstance()
    total = redis_conn.llen('txid_list')
    
    arr.append(total)
    
    index += 1 

    time.sleep(1)
  
  counter = []
  x = 0
  for i in range (0, len(arr)):
    if i == len(arr) - 1: break

    num = arr[i] - arr[i+1]
    counter.append(num) 
    x+= num
  avg = x/sec
  print 'second: %s, count: %s, avg/sec: %s'% (sec, x, avg)
  print 'total: %s, total time: %s Hour' % (total, total/avg/60/60)
  print '-------------------'

