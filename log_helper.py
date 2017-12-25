# -*- encoding: utf-8 -*-

# All rights reserved.
#
# Anti-Therf Log Helper
#
# @author tytymnty@gmail.com
# @since 2015-12-29 12:55:28

import os
import socket
import time
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

datefmt='%Y-%m-%d %H:%M:%S'

LOG_LEVEL = int(os.environ.get("LOG_LEVEL"))
LOG_FILE_PATH = os.environ.get("LOG_FILE_PATH")
LOG_FILE_SIZE = int(os.environ.get("LOG_FILE_SIZE"))
LOG_BACKUP_COUNT = int(os.environ.get("LOG_BACKUP_COUNT"))

FORMAT = "%(asctime)-15s %(name)s %(message)s"
fmt = logging.Formatter(FORMAT, datefmt=datefmt)

class ContextFilter(logging.Filter):
  '''
    Error Log Filter
  '''
  def filter(self, record):
    if record.levelname == 'ERROR':

      fp = '%s.%s'%(LOG_FILE_PATH, record.levelname)
      dt = time.strftime(datefmt, time.gmtime())

      with open (fp, 'w') as f:
        f.write('%s %s\n'% (dt, record.msg))
      
    return True


def get_logger(name):
  """
  Creates a rotating log
  """

  logger = logging.getLogger(name)
  logger.setLevel(LOG_LEVEL)

  # add a rotating handler
  handler = RotatingFileHandler(LOG_FILE_PATH, 
                                maxBytes = LOG_FILE_SIZE,
                                backupCount = LOG_BACKUP_COUNT)

  handler.setFormatter(fmt)
  logger.addHandler(handler)

  f = ContextFilter()
  logger.addFilter(f)
  return logger
