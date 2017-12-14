# -*- encoding: utf-8 -*-

import os
from pymongo import MongoClient
import env_setup

host = os.environ.get("MONGO_HOST")
port = os.environ.get("MONGO_PORT")
username = os.environ.get("MONGO_USER")
password = os.environ.get("MONGO_PASSWORD")

def get_mongo_conn ():
  '''
    get mongodb connection
  '''

  uri = 'mongodb://%s:%s/'%(host, port)
  if username and password:
    uri = 'mongodb://%s:%s@%s:%s/'%(host, port)

  return MongoClient(uri)