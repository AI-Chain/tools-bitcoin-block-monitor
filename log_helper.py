# -*- encoding: utf-8 -*-

# All rights reserved.
#
# Anti-Therf Log Helper
#
# @author tytymnty@gmail.com
# @since 2015-12-29 12:55:28

import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

FORMAT = "%(asctime)-15s %(name)s %(message)s"
fmt = logging.Formatter(FORMAT, datefmt='%Y-%m-%d %H:%M:%S')

def get_logger(name):
  """
  Creates a rotating log
  """

  logger = logging.getLogger(name)
  logger.setLevel(int(os.environ.get("LOG_LEVEL")))

  # add a rotating handler
  handler = RotatingFileHandler(os.environ.get("LOG_FILE_PATH"), 
                                maxBytes = int(os.environ.get("LOG_FILE_SIZE")),
                                backupCount = int(os.environ.get("LOG_BACKUP_COUNT"))

                              )

  handler.setFormatter(fmt)
  logger.addHandler(handler)

  return logger
