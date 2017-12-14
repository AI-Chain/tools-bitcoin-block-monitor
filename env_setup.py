# -*- encoding: utf-8 -*-

# 环境变量加载
# @author tytymnty@gmail.com
# @since 2016-08-01 17:50:53

import os
from os.path import join, dirname
from dotenv import load_dotenv

# 初始化环境变量
dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)