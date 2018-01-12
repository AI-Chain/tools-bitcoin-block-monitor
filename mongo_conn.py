# -*- encoding: utf-8 -*-

from pymongo import MongoClient
import env_setup

def get_mongo_conn (host, port):
  '''
    get mongodb connection
  '''

  uri = 'mongodb://%s:%s/'%(host, port)

  return MongoClient(uri, socketTimeoutMS=3000, connectTimeoutMS=2000, serverSelectionTimeoutMS=2000)