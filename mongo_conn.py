# -*- encoding: utf-8 -*-

import os
from pymongo import MongoClient
import env_setup

host = os.environ.get("MONGO_HOST")
port = os.environ.get("MONGO_PORT")


def get_mongo_conn ():
  '''
    get mongodb connection
  '''

  return MongoClient('mongodb://%s:%s/'%(host, port) )