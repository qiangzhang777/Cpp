# !/usr/bin python3
# -*- encoding:utf-8 -*-
# @Author : Samzhang
# @File : trend_020_t01.py
# @Time : 2023/3/9 6:21


import requests as req
import httpx
import asyncio
import re
import pandas as pd
import time
import tushare as ts
import pymongo as pm
from multiprocessing import Process
from multiprocessing import Queue
import pyautogui as pg
from selenium import webdriver
import tkinter as tk
import math
import random
import os
import sys
import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from proxy_url import Proxy_url
from logger import *
import threading as thr
from auto_login import *
import json
import talib
import numpy as np
from send_2_phone import *
import copy
import pickle
import shelve2
import asyncio
import selfcheck
from scipy.stats import norm
import queue
import profile
from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5.QtCore import QTimer
from control_panel_ui import *
from per_ids_widget import *
import matplotlib
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from memory_profiler import *
import numba
from numba import *

class trend_one:
    def __init__(self, code, obj, ids_selfBk_lst):
        self.code = code

        self.curr_price = 0
        self.curr_time = None
        self.today_lowest = 0
        self.today_highest = 0
        self.ids_mMacd = 0
        self.ids_mDif = 0
        self.ids_kMacd = 0
        self.ids_kDif = 0
        self.ids_kMacd_trd = 0

        self.buy_point = {'x': [], 'p': []}

        self.run_x = -1
        self.m_price_dct = {'t': [], 'p': [], 'x': []}

        self.low_point = {'t': 0, 'p': 1000}
        self.from_low_up_dct = {}
        self.from_low_bt = 0
        self.from_low_flag = False

        self.high_point = {'t': 0, 'p': -1000}
        self.from_high_down_dct = {}
        self.from_high_bt = 0
        self.from_high_flag = False

        self.ids_curr_trend3 = 'None'

        # 获取当前执行文件的名称
        self.file_name = obj.file_name
        # 设置交易时间
        self.today = obj.today
        self.yestoday = obj.yestoday

        # 连接mongoDB
        self.myclient = pm.MongoClient("mongodb://localhost:27017/")
        self.fd = self.myclient["freedom"]

        self.run_type = obj.run_type

        # 启动日志
        # self.logger = Logger('./trading_' + str(self.today) + '.log').get_logger()
        self.data_path = obj.data_path
        if self.run_type == 'trading':
            self.logger_path = f"{self.data_path}/log_trd"
            if not os.path.exists(self.logger_path):
                os.mkdir(self.logger_path)

        elif self.run_type == 'trading_debug':
            self.logger_path = f"{self.data_path}/log_{self.file_name}"
            if not os.path.exists(self.logger_path):
                os.mkdir(self.logger_path)

        if os.path.exists(self.logger_path + self.code + ".log"):
            os.remove(self.logger_path + self.code + ".log")

        self.logger_ids = self.get_trend_one_logger()
        self.logger = obj.logger
        self.logger_ids.debug(self.code)

        # 获取公共数据
        self.trade_lock = obj.trade_lock
        self.dp_bk_lock = obj.dp_bk_lock

        self.sz_price_lst = obj.sz_price_lst
        self.sh_price_lst = obj.sh_price_lst

        self.sz_yestoday = obj.sz_yestoday
        self.sh_yestoday = obj.sh_yestoday

        self.dapan = obj.dapan
        self.dapan_yes1_macd = obj.dapan_yes1_macd
        self.dapan_macd_is_get_smaller = obj.dapan_macd_is_get_smaller
        self.dapan_yes1_beili = obj.dapan_yes1_beili
        self.dapan_xiadie_trend = obj.dapan_xiadie_trend

        self.bk_kline = obj.bk_kline

        self.trend_nums = obj.trend_nums
        self.all_cangwei = obj.all_cangwei
        self.bt1_nums = obj.bt1_nums
        self.bt2_nums = obj.bt2_nums
        self.bt3_nums = obj.bt3_nums

        self.bt1_codes_lst = obj.bt1_codes_lst
        self.bt2_codes_lst = obj.bt2_codes_lst
        self.bt3_codes_lst = obj.bt3_codes_lst
        self.bt4_codes_lst = obj.bt4_codes_lst

        self.data_path = obj.data_path

        self.trade_buy_type = obj.trade_buy_type
        self.trade_sale_type = obj.trade_sale_type

        if self.run_type == 'backtest':
            self.trend_rec = self.fd[self.file_name + 'trend_rec']
            self.hasBuy = self.fd[self.file_name + 'trend_has_buy']
            self.basic_data_store = self.fd[self.file_name + 'trend__basic_data']
            self.trend_reality = self.fd[self.file_name + 'trend_reality']

            # 获取个股的kline close数据，用于计算curr_kMacd
            kline_df = pd.read_excel(f'{self.data_path}/code_kline/{self.code}.xlsx')

            int_yestoday = int(self.yestoday[0:4] + self.yestoday[5:7] + self.yestoday[8:])

            self.bk_name = obj.bk_name
            self.bk_code = obj.bk_code

            self.ids_kline_close = kline_df[kline_df['trade_date'] <= int_yestoday]['close'].iloc[::-1]

            self.ids_kline_close = np.append(self.ids_kline_close, 0)

            # 提前计算板块kline情况，backtest无法包含当日

            self.bk_cond = False
            # 查看板块情况
            self.bk_code_lst = self.bk_code[self.bk_code['code'] == self.code]['bk_code'].values

            for bk_code in self.bk_code_lst:

                # 先判断是否大面积翻红，且mk往上走， 再判断dif,dea,macd 情况
                # 判断当前dif,dea,macd情况
                if len(self.bk_kline[bk_code]) == 0:
                    continue

                bk_kline_dif = self.bk_kline[bk_code].tail(1)['dif1'].values[0]
                bk_kline_dea = self.bk_kline[bk_code].tail(1)['dea1'].values[0]
                if bk_kline_dif > bk_kline_dea:
                    self.bk_cond = True

        elif self.run_type in ['trading', 'trading_debug']:

            if self.run_type == 'trading':
                self.trend_rec = self.fd['trend_rec']
                self.hasBuy = self.fd['trend_has_buy']
                self.trend_reality = self.fd['trend_reality']
                self.bk_curr_rec = self.fd['bk_curr_rec' + self.today]
                self.bk_ids = self.fd['trend_bk_ids']

                # 获取个股的kline close数据，用于计算curr_kMacd
                self.ids_kline_close = pd.read_excel(f'{self.data_path}/code_kline/{self.code}.xlsx')['close'].iloc[::-1]
                self.ids_selfBk_lst = ids_selfBk_lst
                self.trader = obj.trader

            elif self.run_type == 'trading_debug':
                self.trend_rec = self.fd[self.file_name + '_trd_rec']
                self.hasBuy = self.fd[self.file_name + '_trd_has_buy']
                self.basic_data_store = self.fd[self.file_name + '_trd__basic_data']
                self.trend_reality = self.fd[self.file_name + '_trd_reality']
                self.bk_curr_rec = self.fd[self.file_name + '_bk_curr_rec' + self.today]
                self.bk_ids = self.fd[self.file_name + '_bk_ids']

                # 获取个股的kline close数据，用于计算curr_kMacd
                self.ids_kline_close = pd.read_excel(f'{self.data_path}/code_kline/{self.code}.xlsx')['close'].iloc[::-1]
                self.ids_selfBk_lst = ids_selfBk_lst

            self.bk_name = obj.bk_name
            self.bk_code = obj.bk_code

            self.ids_kline_close = np.append(self.ids_kline_close, self.ids_kline_close[0])

        # 记录个股当日购买情况，如果购买了，且当前价格大于购买价格， 那么跳过计算，如果购买了，且当前价格低于购买价格，那么计算（实例对象时，需要查询数据库）
        bt1 = self.hasBuy.find_one({'code': self.code, 'bt': 1, 'buy_date': self.today})
        self.bt1_cost = 0
        if bt1:
            self.bt1_cost = bt1['cost']

        # 捕捉最低和最高价格临时记录字典变量
        self.catch_lowest = pd.Series(dtype='float64')
        self.catch_highest = pd.Series(dtype='float64')
        self.beili = obj.code_ma[self.code]['beili']
        self.trd_days = obj.code_ma[self.code]['trd_days']
        self.sum_ma4 = obj.code_ma[self.code]['sum_ma4']
        self.sum_ma5 = obj.code_ma[self.code]['sum_ma5']
        self.sum_ma6 = obj.code_ma[self.code]['sum_ma6']
        self.sum_ma7 = obj.code_ma[self.code]['sum_ma7']
        self.sum_ma8 = obj.code_ma[self.code]['sum_ma8']
        self.yes2_ma5 = obj.code_ma[self.code]['yes2_ma5']
        self.yes1_ma5 = obj.code_ma[self.code]['yes1_ma5']
        self.yes1_ma4 = obj.code_ma[self.code]['yes1_ma4']
        self.yes2_ma4 = obj.code_ma[self.code]['yes2_ma4']
        self.yes1_ma6 = obj.code_ma[self.code]['yes1_ma6']
        self.yes2_ma6 = obj.code_ma[self.code]['yes2_ma6']
        self.yes1_ma7 = obj.code_ma[self.code]['yes1_ma7']
        self.yes2_ma7 = obj.code_ma[self.code]['yes2_ma7']
        self.yes1_ma8 = obj.code_ma[self.code]['yes1_ma8']
        self.yes2_ma8 = obj.code_ma[self.code]['yes2_ma8']
        self.yes1_close = obj.code_ma[self.code]['yes1_close']
        self.yes2_close = obj.code_ma[self.code]['yes2_close']
        self.yes1_lowest = obj.code_ma[self.code]['yes1_lowest']
        self.yes2_lowest = obj.code_ma[self.code]['yes2_lowest']
        self.yes1_highest = obj.code_ma[self.code]['yes1_highest']
        self.yes2_highest = obj.code_ma[self.code]['yes2_highest']

        self.yes1_macd = obj.code_ma[self.code]['yes1_macd']
        self.yes2_macd = obj.code_ma[self.code]['yes2_macd']

        self.trend3 = obj.code_ma[self.code]['trend3']
        self.trend4 = obj.code_ma[self.code]['trend4']
        self.trend5 = obj.code_ma[self.code]['trend5']
        self.trend6 = obj.code_ma[self.code]['trend6']

        self.trend8 = obj.code_ma[self.code]['trend8']
        self.trend9 = obj.code_ma[self.code]['trend9']
        self.trend10 = obj.code_ma[self.code]['trend10']

        self.trend12 = obj.code_ma[self.code]['trend12']
        self.trend13 = obj.code_ma[self.code]['trend13']
        self.trend14 = obj.code_ma[self.code]['trend14']

        self.trend20 = obj.code_ma[self.code]['trend20']
        self.trend50 = obj.code_ma[self.code]['trend50']

        # 临时存储极值
        self.lowest_price = 1000
        self.highest_price = 0
        # 记录清空标记
        self.clean_flag = False

        # 初始话ser和lastreq, 以及用于计算price, macd, dea, dif trend的容器
        self.ser = pd.Series(dtype='float64')
        self.lastreq = {}
        self.sold_min_dct = {'curr_price': [], 'time': []}
        self.buy_min_dct = {'curr_price': [], 'time': []}
        self.isAppear_top = False

        # 获取个股k线close，用于计算curr_kMacd

        # 设置最大仓位
        self.allow_all_cangwei = obj.allow_all_cangwei
        self.allow_all_topTradeMoney = obj.allow_all_topTradeMoney
        self.per_top_money = obj.per_top_money

        self.pct_sh = 0
        self.pct_sz = 0

        # 设置return 价格，用于判断是否创建jk buy任务
        self.return_price = -1000

        # 初始化最低价格之后的趋势捕捉变量
        self.catch_lowest_mk = 'None'
        self.catch_lowest_mk_val = -10

    def get_trend_one_logger(self):
        self.logger = logging.getLogger(f"logger_ids_{self.code}")
        # 判断是否有处理器，避免重复执行
        if not self.logger.handlers:
            # 日志输出的默认级别为warning及以上级别，设置输出info级别
            self.logger.setLevel(logging.DEBUG)
            # 创建一个处理器handler  StreamHandler()控制台实现日志输出
            sh = logging.StreamHandler()
            # 创建一个格式器formatter  （日志内容：当前时间，文件，日志级别，日志描述信息）
            formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(lineno)d line]: %(message)s')
            sh.setFormatter(formatter)
            # 设置控制台输出级别
            sh.setLevel(logging.DEBUG)

            # 创建一个文件处理器，文件写入日志
            fh = logging.FileHandler(filename=f'{self.logger_path}/{self.code}.log', encoding="utf8")
            # 创建一个文件格式器f_formatter
            f_formatter = logging.Formatter(fmt="[%(asctime)s] [%(levelname)s] [%(lineno)d line]: %(message)s", datefmt="%Y/%m/%d %H:%M:%S")

            # 关联文件日志器-处理器-格式器
            fh.setFormatter(f_formatter)
            # 设置文件输出级别
            fh.setLevel(logging.DEBUG)
            # 关联控制台日志器—处理器—格式器

            self.logger.addHandler(sh)
            self.logger.addHandler(fh)

        return self.logger

    async def jk_buy_am(self, lastreq, curr_time, curr_bk_info=None, curr_dp_info=None):
        t = time.time()
        self.curr_time = curr_time
        self.run_x += 1
        if not lastreq:
            return 0

        # 获取基础数据情况
        # ***************************************************************************
        self.logger_ids.debug(f"{curr_time}:**********************************************************")
        try:
            self.today_lowest = lastreq['today_lowest']
            self.today_highest = lastreq['today_highest']
            self.curr_price = lastreq['curr_price']

            self.m_price_dct['t'].append(str(curr_time.time())[0:8])
            self.m_price_dct['p'].append(self.curr_price)
            self.m_price_dct['x'].append(self.run_x)

            if self.curr_price <= 4.8:
                self.return_price = 4.8
                return 0

            # 捕捉最小价格
            if len(self.catch_lowest.values) > 1:
                pre1_price = self.catch_lowest.values[-1]
            else:
                pre1_price = 0

            if self.lowest_price > self.today_lowest:
                self.lowest_price = self.today_lowest
                self.catch_lowest = pd.Series(dtype='float64')
                self.catch_lowest[curr_time] = self.today_lowest
                # self.logger.info(f"lowest_point!!!: {self.lowest_price}")
                self.logger_ids.debug(f"catch_lowest:{self.catch_lowest}")

                self.low_point['x'] = self.run_x
                self.low_point['p'] = self.today_lowest

                # 如果出现新低， 更新前面添加的值，有可能curr_price与today_lowest不一致
                self.m_price_dct['p'][-1] = self.today_lowest

                if self.from_low_bt in self.from_low_up_dct.keys():
                    self.from_low_up_dct[self.from_low_bt]['t'].clear()
                    self.from_low_up_dct[self.from_low_bt]['x'].clear()
                    self.from_low_up_dct[self.from_low_bt]['p'].clear()
                else:
                    self.from_low_up_dct[self.from_low_bt] = {'t': [curr_time], 'p': [self.today_lowest], 'x': [self.run_x]}

                self.from_low_flag = True

            elif len(self.catch_lowest.values) >= 1 and self.catch_lowest.values[0] == self.today_lowest:
                self.catch_lowest[curr_time] = self.curr_price
                if self.from_low_flag == True:
                    self.from_low_up_dct[self.from_low_bt]['t'].append(curr_time)
                    self.from_low_up_dct[self.from_low_bt]['p'].append(self.curr_price)
                    self.from_low_up_dct[self.from_low_bt]['x'].append(self.run_x)
                try:
                    if self.from_low_flag == True and (self.catch_lowest.values[-1] - self.catch_lowest.values[0]) / self.catch_lowest.values[0] > 0.008:
                        self.from_low_flag = False
                        self.from_low_bt += 1
                except Exception as e:
                    print(e)

            temp_catch_loest_mk_val = 0
            # print(f"{self.code}:{len(self.catch_lowest.values)},{self.catch_lowest_mk}", end='| ')
            if len(self.catch_lowest.values) >= 50:
                if self.catch_lowest_mk == 'incs' and self.curr_price >= pre1_price:
                    pass
                elif self.catch_lowest_mk == 'decs' and self.curr_price <= pre1_price:
                    pass
                elif self.catch_lowest_mk == 'notrend' and self.curr_price == pre1_price:
                    pass
                else:
                    temp_catch_lowest = mk(self.catch_lowest.values)
                    self.catch_lowest_mk = temp_catch_lowest[0]
                    self.catch_lowest_mk_val = temp_catch_lowest[1]

                    if round(self.catch_lowest_mk_val, 1) != temp_catch_loest_mk_val:
                        temp_catch_loest_mk_val = self.catch_lowest_mk_val
                        self.logger_ids.debug(f"catch_lowest_mk: {self.catch_lowest_mk}, catch_lowest_mk_val: {self.catch_lowest_mk_val}")

            # 记录个股当日分时数据，趋势情况
            self.buy_min_dct['time'].append(str(curr_time))
            self.buy_min_dct['curr_price'].append(self.curr_price)

            if len(self.buy_min_dct['curr_price']) > 128:
                self.buy_min_dct['curr_price'].pop(0)
                self.buy_min_dct['time'].pop(0)

        except Exception as e:
            self.logger_ids.error(f"error1: {lastreq}, {e}")

        # 计算个股kline macd
        # ***************************************************************************
        # 当前curr_price 结合之前 kline的macd数据
        self.ids_kline_close[-1] = self.curr_price
        ids_kline_dif, ids_kline_dea, ids_kline_dw = talib.MACD(self.ids_kline_close, 3, 8, 5)
        ids_kline_macd = (ids_kline_dif - ids_kline_dea) * 2

        ids_curr_kDif = ids_kline_dif[-1]
        ids_curr_kDea = ids_kline_dea[-1]
        ids_curr_kMacd = ids_kline_macd[-1]
        ids_curr_kMacd_trd = mk(ids_kline_macd[-3:])[0]

        self.ids_kMacd = ids_curr_kMacd
        self.ids_kDif = ids_curr_kDif
        self.ids_kMacd_trd = ids_curr_kMacd_trd

        # 获取当前价格trend3
        self.ids_curr_low_mk = mk([self.yes2_lowest, self.yes1_lowest, self.lowest_price], 0.5)[1]
        self.ids_curr_high_mk = mk([self.yes2_highest, self.yes1_highest, self.highest_price], 0.5)[1]
        self.ids_curr_close_mk = mk([self.yes2_close, self.yes1_close, self.curr_price], 0.5)[1]

        if self.ids_curr_low_mk + self.ids_curr_high_mk + self.ids_curr_close_mk >= 2:
            self.ids_curr_trend3 = 'incs'
        elif self.ids_curr_low_mk + self.ids_curr_high_mk + self.ids_curr_close_mk <= -2:
            self.ids_curr_trend3 = 'decs'
        else:
            self.ids_curr_trend3 = 'noTrend'

        # 判断个股退出条件
        # ***************************************************************************
        # 如果第一次价格不为零，且当前价格大于了第一次买入价格，那么后面不需要再做买入操作
        if self.bt1_cost != 0:
            # 使用self.pre_price 避免频繁的更新数据库，如果上一次价格和当前价格不一样才做更新
            if self.run_type == 'trading_debug' and self.pre_price != self.curr_price:
                self.pre_price = self.curr_price
                hasBuy_res = self.hasBuy.find({'code': self.code, 'buy_date': self.today})
                for r in hasBuy_res:
                    yingkui = (self.curr_price - r['cost']) * r['num']
                    pct_bt = round(yingkui / r['money'], 4)
                    self.hasBuy.update_one({'_id': r['_id']}, {'$set': {'curr_price': self.curr_price, 'yingkui': yingkui, 'pct_bt': pct_bt}})
            if self.curr_price >= self.bt1_cost:
                return 0
        else:
            self.pre_price = 0

        # 判断当前价格是否有效
        if self.curr_price == 0:
            return 0

        # 判断是否出现顶背离 对齐：bt112_2.py
        self.top_beili_cond1 = not (self.yes2_lowest < self.yes1_lowest and self.yes2_highest < self.yes1_highest)
        self.top_beili_cond2 = (self.yes1_ma7 > self.yes2_ma7 or self.yes1_ma6 > self.yes2_ma6 or self.yes1_ma8 > self.yes2_ma8)
        self.top_beili_cond3 = (self.yes1_ma4 > self.curr_price or self.yes1_ma5 > self.curr_price or self.yes1_ma6 > self.curr_price or self.yes1_ma7 > self.curr_price)
        if self.top_beili_cond1 and self.top_beili_cond2 and self.top_beili_cond3:
            self.isAppear_top = True
            self.return_price = self.curr_price
            self.logger_ids.debug(f"check isAppear_top: {self.isAppear_top}, return")
            return 0
        else:
            self.logger_ids.debug(f"check isAppear_top: {self.isAppear_top}, next")

        # 如果你是底部，但是现在价格低于了这个底部，那就不能买，应该全卖出
        if self.yes1_lowest > self.curr_price:
            self.return_price = self.yes1_lowest
            self.logger_ids.debug(f"check self.yes1_lowest({self.yes1_lowest}) > self.curr_price({self.curr_price}), return")
            return 0
        else:
            self.logger_ids.debug(f"check self.yes1_lowest({self.yes1_lowest}) > self.curr_price({self.curr_price}), next")

        # 判断大盘和板块退出条件
        # ***************************************************************************
        # 如果大盘和行业板块都不好则退出，trading结合当日curr_price, backtest结合昨日kline
        if self.run_type == 'trading' or self.run_type == 'trading_debug':
            # 获取板块情况
            self.bk_cond = False
            self.bk_code_lst = self.bk_code[self.bk_code['code'] == self.code]['bk_code'].values
            ids_ok_bk_code = ''

            for bk_code in self.bk_code_lst:
                try:
                    if bk_code in curr_bk_info.keys():
                        self.logger_ids.debug(f"""check {self.code}'s:{bk_code} curr_kMacd({round(curr_bk_info[bk_code]['curr_kMacd'], 2)}) > 0 and  curr_kMacd_trend({curr_bk_info[bk_code]['curr_kMacd_trend']}) == 'incs') and  ((min_macd({curr_bk_info[bk_code]['min_macd']}) > 0 and min_macd_trend({curr_bk_info[bk_code]['min_macd_trend']}) == 'incs' and min_dif({curr_bk_info[bk_code]['min_dif']}) > 0) or price_trend({curr_bk_info[bk_code]['price_trend']}) == 'incs')""")

                        if ((curr_bk_info[bk_code]['curr_kMacd'] > 0 and curr_bk_info[bk_code]['curr_kMacd_trend'] == 'incs') or \
                                (curr_bk_info[bk_code]['trend3'] == 'incs' and curr_bk_info[bk_code]['curr_kMacd_trend'] != 'decs')):
                                # ((curr_bk_info[bk_code]['min_macd'] > 0 and curr_bk_info[bk_code]['min_macd_trend'] == 'incs' and curr_bk_info[bk_code]['min_dif'] > 0) or curr_bk_info[bk_code]['price_trend'] == 'incs'):

                            # 这里判断符合标准的板块，个股买入情况
                            bk_ids_r = self.bk_ids.find_one({'date': self.today, 'bk_code': bk_code})
                            if bk_ids_r:
                                if bk_ids_r['hasBuy_nums'] < bk_ids_r['idsNums_allowed_buy'] and self.code not in bk_ids_r['hasBuy_lst']:
                                    self.logger_ids.debug(f"check {bk_code} hasBuy, hasBuy_nums({bk_ids_r['hasBuy_nums']}) < idsNums_allowed_buy({bk_ids_r['idsNums_allowed_buy']}) and self.code not in hasBuy_lst")
                                    ids_ok_bk_code = bk_code
                                    self.bk_cond = True
                                    self.logger_ids.debug(f"{bk_code} self.bk_cond == True")
                                    # 有一个板块满足条件，就退出板块循环
                                    break
                                else:
                                    self.logger_ids.debug(f"check {bk_code} hasBuy, hasBuy_nums({bk_ids_r['hasBuy_nums']}) < idsNums_allowed_buy({bk_ids_r['idsNums_allowed_buy']}) and {self.code} not in {str(bk_ids_r['hasBuy_lst'])}")
                            else:
                                self.logger_ids.debug(f"no find {bk_code} in bk_ids mongodb")

                        # 如果已经购买第一次，第二次购买的话，放宽板块购买条件，只要板块没有走差，即可进行补仓
                        if self.bt1_cost != 0 and ((curr_bk_info[bk_code]['curr_kMacd'] > 0 or curr_bk_info[bk_code]['trend3'] == 'incs') and curr_bk_info[bk_code]['curr_kMacd_trend'] != 'decs'):
                            self.bk_cond = True
                            self.logger_ids.debug(f"check {bk_code} ,self.bt1_cost({self.bt1_cost}) != 0 and (curr_bk_info[bk_code]['curr_kMacd']({curr_bk_info[bk_code]['curr_kMacd']}) > 0 and curr_bk_info[bk_code]['curr_kMacd_trend']({curr_bk_info[bk_code]['curr_kMacd_trend']}) != 'decs')")
                            self.logger_ids.debug(f"{bk_code} self.bk_cond == True")
                            break
                        else:
                            self.logger_ids.debug(f"check {bk_code} ,self.bt1_cost({self.bt1_cost}) != 0 and (curr_bk_info[bk_code]['curr_kMacd']({curr_bk_info[bk_code]['curr_kMacd']}) > 0 and curr_bk_info[bk_code]['curr_kMacd_trend']({curr_bk_info[bk_code]['curr_kMacd_trend']}) != 'decs')")
                except Exception as e:
                    self.logger_ids.error(f"{self.code}:, {curr_time}, return by curr_bk_info:({curr_bk_info})")
                    self.logger_ids.error(f"{self.code}:, {curr_time}, return by error:({e})")
                    return 0

            if self.bk_cond == False:
                self.logger_ids.debug(f"{self.code}:, {curr_time}, return by bk_cond:self.bk_cond({self.bk_cond})")
                return 0

        elif self.run_type == 'backtest':
            # 大盘dif<dea,或者顶背离，或者下跌趋势，且板块dif<dea那么退出
            if ((self.sh_yestoday['dif1'].values[0] < self.sh_yestoday['dea1'].values[0]) or (self.sz_yestoday['dif1'].values[0] < self.sz_yestoday['dea1'].values[0]) or self.dapan_yes1_beili == 'top' or self.dapan_xiadie_trend) and self.bk_cond == False:
                return 0

        try:
            # 计算个股kline macd
            # ***************************************************************************
            # 当前curr_price 结合之前 kline的macd数据
            # self.ids_kline_close[-1] = self.curr_price
            # ids_kline_dif, ids_kline_dea, ids_kline_dw = talib.MACD(self.ids_kline_close, 3, 8, 5)
            # ids_kline_macd = (ids_kline_dif - ids_kline_dea) * 2
            #
            # ids_curr_kDif = ids_kline_dif[-1]
            # ids_curr_kDea = ids_kline_dea[-1]
            # ids_curr_kMacd = ids_kline_macd[-1]
            # ids_curr_kMacd_trd = mk(ids_kline_macd[-3:])[0]
            #
            # self.ids_kMacd = ids_curr_kMacd
            # self.ids_kDif = ids_curr_kDif
            # self.ids_kMacd_trd = ids_curr_kMacd_trd

            if self.bt1_cost == 0 and not (ids_curr_kMacd > 0 and ids_curr_kMacd_trd != 'decs'):
                self.return_price = self.curr_price
                self.logger_ids.debug(f"check ids kline_macd :self.bt1_cost({self.bt1_cost}) == 0 and not (ids_curr_kMacd({ids_curr_kMacd}) > 0 and ids_curr_kMacd_trd({ids_curr_kMacd_trd}) != 'decs'), return")
                return 0
            elif self.bt1_cost != 0 and (ids_curr_kMacd < 0 or ids_curr_kMacd_trd == 'decs'):
                self.logger_ids.debug(f"check ids kline_macd : self.bt1_cost({self.bt1_cost}) != 0 and (ids_curr_kMacd({ids_curr_kMacd}) < 0 or ids_curr_kMacd_trd({ids_curr_kMacd_trd}) == 'decs'), return")
                return 0
            else:
                self.logger_ids.debug(f"check ids kline_macd : self.bt1_cost({self.bt1_cost}) and (ids_curr_kMacd({ids_curr_kMacd}) or ids_curr_kMacd_trd({ids_curr_kMacd_trd})), next")

            # 计算个股分时macd情况
            if len(self.buy_min_dct['curr_price']) < 5:
                return 0
            ids_mDif_lst, ids_mDea_lst, ids_mDw_lst = talib.MACD(np.array(self.buy_min_dct['curr_price']), 28, 68, 18)
            ids_mDif, ids_mDea, ids_mDw = ids_mDif_lst[-1], ids_mDea_lst[-1], ids_mDw_lst[-1]
            ids_mMacd_lst = (ids_mDif_lst - ids_mDea_lst) * 2
            ids_mMacd = ids_mMacd_lst[-1]
            self.ids_mMacd = ids_mMacd
            self.ids_mDif = ids_mDif
            if len(ids_mMacd_lst[-28:]) >= 3:
                ids_mMacd_trd = mk(ids_mMacd_lst[-28:])[0]
            else:
                ids_mMacd_trd = None

            # 开盘macd还未计算出来时，使用price_trend
            price_trend = ''
            # if curr_time.hour == 9 and curr_time.minute <= 45:
            if np.isnan(ids_mMacd) and len(self.buy_min_dct['curr_price']) >= 28:
                price_trend = mk(self.buy_min_dct['curr_price'])[0]

            self.buy_cond1 = ((self.bt1_cost == 0 and (ids_curr_kMacd > 0 and ids_curr_kMacd_trd != 'decs')) or (self.bt1_cost != 0 and (ids_curr_kMacd > 0 and ids_curr_kMacd_trd != 'decs'))) and ((ids_mMacd > 0 and ids_mDif > 0 and ids_mMacd_trd == 'incs') or (price_trend == 'incs') or (self.catch_lowest_mk == 'incs' and self.catch_lowest_mk_val >= 0.8))

            self.logger_ids.debug(f"""check buy_cond1: self.buy_cond1 = ((self.bt1_cost({self.bt1_cost}) == 0 and (ids_curr_kMacd({ids_curr_kMacd}) > 0 and ids_curr_kMacd_trd({ids_curr_kMacd_trd}) != 'decs')) or 
                                        (self.bt1_cost({self.bt1_cost}) != 0 and (ids_curr_kMacd({ids_curr_kMacd}) > 0 and ids_curr_kMacd_trd({ids_curr_kMacd_trd}) != 'decs'))) and 
                                        ((ids_mMacd({ids_mMacd}) > 0 and ids_mDif({ids_mDif}) > 0 and ids_mMacd_trd({ids_mMacd_trd}) == 'incs') or (price_trend({price_trend}) == 'incs') or 
                                        (self.catch_lowest_mk({self.catch_lowest_mk}) == 'incs' and self.catch_lowest_mk_val({self.catch_lowest_mk_val}) >= 0.8))""")
            if self.buy_cond1 == False:
                self.logger_ids.debug(f"{self.code}:, {curr_time}, return by: self.buy_cond1 == False")
                self.return_price = self.curr_price
                return 0

            # 查询是否已经买入过
            self.hasBuy_count = self.hasBuy.count_documents({'code': self.code, 'buy_date': self.today})
            luocha = 0.0046

            self.buy_cond4 = False
            if self.hasBuy_count == 0 and (not ((self.today_highest - self.today_lowest) / self.today_lowest >= 0.036 and (self.today_highest - self.curr_price) / self.curr_price <= 0.008)):
                self.buy_cond4 = True
                self.logger_ids.debug(f"check buy_cond4: self.buy_cond4 = True: self.hasBuy_count == 0 and (not ((self.today_highest-self.today_lowest)/self.today_lowest({(self.today_highest - self.today_lowest) / self.today_lowest}) >= 0.036 and (self.today_highest - self.curr_price)/self.curr_price({(self.today_highest - self.curr_price) / self.curr_price}) <= 0.008))")
            elif self.hasBuy_count >= 1:
                if len(self.catch_lowest) > 50:
                    lowest_trend = mk(self.catch_lowest)[0]
                    print(f"{self.code}, lowest_trend:{lowest_trend}")
                    self.buy_cond4 = (lowest_trend == 'incs') and (self.catch_lowest.iloc[0] == self.today_lowest) and (self.catch_lowest.max() == self.catch_lowest.iloc[-1]) and 0.003 <= (self.catch_lowest.iloc[-1] - self.today_lowest) / self.today_lowest
                    self.logger_ids.debug(f"check buy_cond4: self.buy_cond4 = (lowest_trend({lowest_trend}) == 'incs') and (self.catch_lowest.iloc[0]({self.catch_lowest.iloc[0]}) == self.today_lowest({self.today_lowest})) and (self.catch_lowest.max()({self.catch_lowest.max()}) == self.catch_lowest.iloc[-1])({self.catch_lowest.iloc[-1]}) and 0.003 <= (self.catch_lowest.iloc[-1] - self.today_lowest) / self.today_lowest({(self.catch_lowest.iloc[-1] - self.today_lowest) / self.today_lowest})")
                else:
                    self.buy_cond4 = False
                    self.logger_ids.debug(f"check buy_cond4: len(self.catch_lowest)({len(self.catch_lowest)}) > 50")
            else:
                self.logger_ids.debug(f"check buy_cond4: self.buy_cond4 = False")
                self.buy_cond4 = False

            self.logger_ids.debug(f"check buy_cond1({self.buy_cond1}) and buy_cond4({self.buy_cond4})")
            if self.buy_cond1 and self.buy_cond4:
                self.act = 'buy'
                # 更新all_cangwei
                self.update_all_cangwei()

                # 因为整体仓位达到上限，导致不能交易
                if self.all_cangwei >= self.allow_all_cangwei:
                    self.logger_ids.debug(f"check cangwei: self.all_cangwei({self.all_cangwei}) >= self.allow_all_cangwei({self.all_cangwei})")
                    return 0

                # 查询bt购买情况，设置购买金额
                self.get_bts()

                # if self.bt1_nums > 8 and (self.code not in self.bt1_codes_lst):
                #     return 0
                # if self.bt2_nums > 4 and (self.code not in self.bt2_codes_lst):
                #     return 0
                # if self.bt3_nums > 2 and (self.code not in self.bt3_codes_lst):
                #     return 0

                if (curr_dp_info['sh']['curr_macd'] < 0 and curr_dp_info['sh']['curr_dif'] < 0) or (curr_dp_info['sh']['mk_kline_macd'] == 'desc') or (self.dapan_yes1_beili == 'top'):
                    self.money1 = self.per_top_money * 2
                    self.money2 = 0
                    self.money3 = 0
                    self.money4 = 0
                    self.money5 = 0
                else:
                    self.money1 = self.per_top_money * 3
                    self.money2 = 0
                    self.money3 = 0
                    self.money4 = 0
                    self.money5 = 0

                #
                # if self.first_buy_nums <= 12:
                #     if curr_dp_info['sh']['curr_macd'] < 0 and curr_dp_info['sh']['curr_dif'] < 0 and curr_dp_info['sh']['mk_kline_macd'] == 'desc' and self.dapan_yes1_beili == 'top':
                #         self.money1 = self.per_top_money
                #         self.money2 = self.per_top_money
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0
                #     elif curr_dp_info['sh']['curr_macd'] > 0 and curr_dp_info['sh']['curr_dif'] < 0 and (not self.dapan_macd_is_get_smaller and not self.dapan_yes1_beili == 'top'):
                #         self.money1 = self.per_top_money * 3
                #         self.money2 = self.per_top_money * 3
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0
                #     else:
                #         self.money1 = self.per_top_money * 1.5
                #         self.money2 = self.per_top_money * 3
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0
                # elif self.first_buy_nums <= 20:
                #     if curr_dp_info['sh']['curr_macd'] < 0 and (self.dapan_macd_is_get_smaller or self.dapan_yes1_beili == 'top'):
                #         self.money1 = 0
                #         self.money2 = 0
                #         self.money3 = self.per_top_money * 1
                #         self.money4 = self.per_top_money * 1
                #         self.money5 = 0
                #     elif curr_dp_info['sh']['curr_macd'] > 0 and curr_dp_info['sh']['curr_dif'] > 0 and (not self.dapan_macd_is_get_smaller and not self.dapan_yes1_beili == 'top'):
                #         self.money1 = self.per_top_money * 2
                #         self.money2 = self.per_top_money * 2
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0
                #     else:
                #         self.money1 = self.per_top_money * 1
                #         self.money2 = self.per_top_money * 2
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0
                # else:
                #     if curr_dp_info['sh']['curr_macd'] < 0 and (self.dapan_macd_is_get_smaller or self.dapan_yes1_beili == 'top'):
                #         self.money1 = 0
                #         self.money2 = 0
                #         self.money3 = self.per_top_money * 1
                #         self.money4 = self.per_top_money * 1
                #         self.money5 = 0
                #     elif curr_dp_info['sh']['curr_macd'] > 0 and curr_dp_info['sh']['curr_dif'] > 0 and (not self.dapan_macd_is_get_smaller and not self.dapan_yes1_beili == 'top'):
                #         self.money1 = self.per_top_money * 1.5
                #         self.money2 = self.per_top_money * 1.5
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0
                #     else:
                #         self.money1 = self.per_top_money * 1
                #         self.money2 = self.per_top_money * 2
                #         self.money3 = 0
                #         self.money4 = 0
                #         self.money5 = 0

                # 查询时间

                if self.hasBuy_count == 0 and self.act == 'buy':
                    # 查看个股所在板块 是否已经购买 超标
                    bk_ids_r = self.bk_ids.find_one({'date': self.today, 'bk_code': ids_ok_bk_code})
                    if bk_ids_r:
                        if bk_ids_r['hasBuy_nums'] < bk_ids_r['idsNums_allowed_buy'] and self.code not in bk_ids_r['hasBuy_lst']:
                            new_hasBuy_lst = bk_ids_r['hasBuy_lst'] + [self.code]
                            new_hasBuy_nums = bk_ids_r['hasBuy_nums'] + 1
                            self.bk_ids.update_one({'date': self.today, 'bk_code': ids_ok_bk_code}, {'$set': {'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums}})

                            print('bk_ids update:', {'code:': self.code, 'bk_code': ids_ok_bk_code, 'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums})
                            self.logger_ids.debug(f"bk_ids update: bk_code({ids_ok_bk_code}), hasBuy +1")
                    else:
                        self.logger_ids.debug(f"check bk_ids_r: return")
                        return 0

                    # if self.first_buy_nums > 28:
                    #     return 0
                    try:
                        self.money = self.money1

                        # if money + self.all_cangwei > self.allow_all_topTradeMoney and self.all_cangwei < self.allow_all_topTradeMoney:
                        #     money = self.allow_all_topTradeMoney - self.all_cangwei

                        # 计算成本单价
                        self.num = (round(self.money / self.curr_price / 100) * 100)

                        if self.num == 0 and self.money != 0:
                            self.num = 100
                            self.buyMoney = self.num * self.curr_price
                            self.cost = (self.buyMoney + 20) / self.num
                        elif self.num == 0 and self.money == 0:
                            self.buyMoney = 0
                            self.cost = 0
                        else:
                            self.buyMoney = self.num * self.curr_price
                            self.cost = (self.buyMoney + 20) / self.num

                        self.bt = 1

                        # 更新仓位情况
                        self.all_cangwei += self.buyMoney

                        # self.autoBuy(ndict, money, 'sale1')
                        self.buy_price = self.curr_price + 0.01
                        if self.money != 0:
                            if self.run_type == 'trading':
                                with self.trade_lock:
                                    res = self.trader.auto_buy_session(self.code, self.buy_price, self.num)
                                    try:
                                        res_dct = json.loads(str(res))
                                        if res_dct['Status'] == 0:
                                            self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                            self.buy_point['x'].append(self.run_x)
                                            self.buy_point['p'].append(self.curr_price)
                                    except Exception as e:
                                        self.logger.error(str(e) + ' 返回结果：' + str(res))
                            elif self.run_type in ['trading_debug', 'backtest']:
                                res = 'backtest'
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                self.buy_point['x'].append(self.run_x)
                                self.buy_point['p'].append(self.curr_price)
                        else:
                            res = 'self.money == 0'
                            self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                            self.buy_point['x'].append(self.run_x)
                            self.buy_point['p'].append(self.curr_price)

                        self.logger_ids.debug(f"{self.code}:, {curr_time}, bt1买入, 单价:{self.buy_price}  数量:{self.num} 返回结果：{res}")

                        self.bt1_cost = self.cost
                        self.catch_lowest = pd.Series(dtype='float64')

                    except Exception as e:
                        self.logger_ids.error(f"error: {self.code}, {e}")

                elif self.hasBuy_count == 1 and self.act == 'buy':

                    # 判断间隔时间
                    try:
                        res = self.hasBuy.find_one({'code': self.code, 'buy_date': self.today, 'bt': 1})
                        t1 = datetime.datetime.strptime(res['time'][0:19], '%Y-%m-%d %H:%M:%S')
                        self.logger_ids.debug(f"{self.code}:, {curr_time}, in bt2:((curr_time - t1).total_seconds()({(curr_time - t1).total_seconds()}) > 480 and (res['price'] - self.curr_price) / res['price']({(res['price'] - self.curr_price) / res['price']}) >= luocha) or res['money']({res['money']}) == 0")
                        if ((curr_time - t1).total_seconds() > 480 and (res['price'] - self.curr_price) / res['price'] >= luocha) or res['money'] == 0:

                            self.money = self.money2
                            # if money + self.all_cangwei > self.allow_all_topTradeMoney and self.all_cangwei < self.allow_all_topTradeMoney:
                            #     money = self.allow_all_topTradeMoney - self.all_cangwei

                            # 计算成本单价
                            self.num = (round(self.money / self.curr_price / 100) * 100)

                            if self.num == 0 and self.money != 0:
                                self.num = 100
                                self.buyMoney = self.num * self.curr_price
                                self.cost = (self.buyMoney + 20) / self.num
                            elif self.num == 0 and self.money == 0:
                                self.buyMoney = 0
                                self.cost = 0
                            else:
                                self.buyMoney = self.num * self.curr_price
                                self.cost = (self.buyMoney + 20) / self.num

                            self.bt = 2

                            # 更新仓位情况
                            self.all_cangwei += self.buyMoney

                            # self.autoBuy(ndict, money, 'sale1')
                            self.buy_price = self.curr_price + 0.01

                            if self.money != 0:

                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        # res = self.trader.auto_buy_chrome(self.code, buy_price, num)
                                        res = self.trader.auto_buy_session(self.code, self.buy_price, self.num)
                                        try:
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] == 0:
                                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                                self.buy_point['x'].append(self.run_x)
                                                self.buy_point['p'].append(self.curr_price)
                                        except Exception as e:
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                    self.buy_point['x'].append(self.run_x)
                                    self.buy_point['p'].append(self.curr_price)
                                    res = 'backtest'

                            else:
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                self.buy_point['x'].append(self.run_x)
                                self.buy_point['p'].append(self.curr_price)
                                res = 'self.money == 0'

                            self.logger_ids.debug(f"{self.code}:, {curr_time}, bt2买入, 单价:{self.buy_price}  数量:{self.num} 返回结果：{res}")

                            self.catch_lowest = pd.Series(dtype='float64')
                    except Exception as e:
                        self.logger_ids.error(f"error: {self.code}, {e}")

                elif self.hasBuy_count == 2 and self.act == 'buy':

                    self.money = self.money3

                    if self.money == 0:
                        return 0

                    # 判断间隔时间
                    try:
                        res = self.hasBuy.find_one({'code': self.code, 'buy_date': self.today, 'bt': 2})
                        t1 = datetime.datetime.strptime(res['time'][0:19], '%Y-%m-%d %H:%M:%S')

                        self.logger_ids.debug(f"{self.code}:, {curr_time}, in bt3:((curr_time - t1).total_seconds()({(curr_time - t1).total_seconds()}) > 480 and (res['price'] - self.curr_price) / res['price']({(res['price'] - self.curr_price) / res['price']}) >= luocha)")
                        if (curr_time - t1).total_seconds() > 480 and (res['price'] - self.curr_price) / res['price'] >= luocha:

                            # if money + self.all_cangwei > self.allow_all_topTradeMoney and self.all_cangwei < self.allow_all_topTradeMoney:
                            #     money = self.allow_all_topTradeMoney - self.all_cangwei

                            # 计算成本单价
                            self.num = (round(self.money / self.curr_price / 100) * 100)

                            if self.num == 0 and self.money != 0:
                                self.num = 100
                                self.buyMoney = self.num * self.curr_price
                                self.cost = (self.buyMoney + 20) / self.num
                            elif self.num == 0 and self.money == 0:
                                self.buyMoney = 0
                                self.cost = 0
                            else:
                                self.buyMoney = self.num * self.curr_price
                                self.cost = (self.buyMoney + 20) / self.num

                            self.bt = 3
                            # 更新仓位情况
                            self.all_cangwei += self.buyMoney

                            # self.autoBuy(ndict, money, 'sale1')
                            self.buy_price = self.curr_price + 0.01
                            if self.money != 0:
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        # res = self.trader.auto_buy_chrome(self.code, buy_price, num)
                                        res = self.trader.auto_buy_session(self.code, self.buy_price, self.num)
                                        try:
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] == 0:
                                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                                self.buy_point['x'].append(self.run_x)
                                                self.buy_point['p'].append(self.curr_price)
                                        except Exception as e:
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                elif self.run_type in ['trading_debug', 'backtest']:

                                    self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                    self.buy_point['x'].append(self.run_x)
                                    self.buy_point['p'].append(self.curr_price)
                                    res = 'backtest'

                                self.logger.info(self.code + ' bt3买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))

                            else:
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                self.buy_point['x'].append(self.run_x)
                                self.buy_point['p'].append(self.curr_price)
                                res = 'self.money == 0'
                            self.logger_ids.debug(f"{self.code}:, {curr_time}, bt3买入, 单价:{self.buy_price}  数量:{self.num} 返回结果：{res}")

                            self.catch_lowest = pd.Series(dtype='float64')
                    except Exception as e:
                        self.logger_ids.error(f"error: {self.code}, {e}")

                elif self.hasBuy_count == 3 and self.act == 'buy':

                    self.money = self.money4

                    if self.money == 0:
                        return 0

                    # 判断间隔时间
                    try:
                        res = self.hasBuy.find_one({'code': self.code, 'buy_date': self.today, 'bt': 3})
                        t1 = datetime.datetime.strptime(res['time'][0:19], '%Y-%m-%d %H:%M:%S')

                        self.logger_ids.debug(f"{self.code}:, {curr_time}, in bt4:((curr_time - t1).total_seconds()({(curr_time - t1).total_seconds()}) > 480 and (res['price'] - self.curr_price) / res['price']({(res['price'] - self.curr_price) / res['price']}) >= luocha)")
                        if (curr_time - t1).total_seconds() > 480 and (res['price'] - self.curr_price) / res['price'] >= luocha:

                            # if money + self.all_cangwei > self.allow_all_topTradeMoney and self.all_cangwei < self.allow_all_topTradeMoney:
                            #     money = self.allow_all_topTradeMoney - self.all_cangwei

                            # 计算成本单价
                            self.num = (round(self.money / self.curr_price / 100) * 100)

                            if self.num == 0 and self.money != 0:
                                self.num = 100
                                self.buyMoney = self.num * self.curr_price
                                self.cost = (self.buyMoney + 20) / self.num
                            elif self.num == 0 and self.money == 0:
                                self.buyMoney = 0
                                self.cost = 0
                            else:
                                self.buyMoney = self.num * self.curr_price
                                self.cost = (self.buyMoney + 20) / self.num

                            self.bt = 4
                            # 更新仓位情况
                            self.all_cangwei += self.buyMoney

                            # self.autoBuy(ndict, money, 'sale1')
                            self.buy_price = self.curr_price + 0.01
                            if self.money != 0:
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        # res = self.trader.auto_buy_chrome(k, buy_price, num)
                                        res = self.trader.auto_buy_session(k, self.buy_price, self.num)
                                        try:
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] == 0:
                                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                                self.buy_point['x'].append(self.run_x)
                                                self.buy_point['p'].append(self.curr_price)
                                        except Exception as e:
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                    self.buy_point['x'].append(self.run_x)
                                    self.buy_point['p'].append(self.curr_price)
                                    res = 'backtest'

                                self.logger.info(self.code + ' bt4买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))
                            else:
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                self.buy_point['x'].append(self.run_x)
                                self.buy_point['p'].append(self.curr_price)
                                res = 'self.money == 0'
                            self.logger_ids.debug(f"{self.code}:, {curr_time}, bt4买入, 单价:{self.buy_price}  数量:{self.num} 返回结果：{res}")

                            self.catch_lowest = pd.Series(dtype='float64')
                    except Exception as e:
                        self.logger_ids.error(f"error: {self.code}, {e}")

        except Exception as e:
            raise e
            self.logger_ids.error(self.code + ' error :' + str(e))

    async def jk_sale_am(self, lastreq, curr_time, curr_bk_info=None, curr_dp_info=None):
        self.curr_time = curr_time
        if not lastreq:
            return 0

        try:
            today_highest = lastreq['today_highest']
            today_lowest = lastreq['today_lowest']
            curr_price = lastreq['curr_price']
            curr_pct_chg = lastreq['pct_chg']
        except Exception as e:
            self.logger.error(f"lastreq:{lastreq}, {e}")

        self.sold_min_dct['time'].append(str(curr_time))
        self.sold_min_dct['curr_price'].append(curr_price)
        if len(self.sold_min_dct['curr_price']) > 128:
            self.sold_min_dct['curr_price'].pop(0)
            self.sold_min_dct['time'].pop(0)

        # 判断当前价格是否有效
        if curr_price == 0:
            return 0

        # 当前curr_price 结合之前 kline的macd数据
        self.ids_kline_close[-1] = curr_price
        ids_kline_dif, ids_kline_dea, ids_kline_dw = talib.MACD(self.ids_kline_close, 3, 8, 5)
        ids_kline_macd = (ids_kline_dif - ids_kline_dea) * 2

        ids_curr_kDif = ids_kline_dif[-1]
        ids_curr_kDea = ids_kline_dea[-1]
        ids_curr_kMacd = ids_kline_macd[-1]
        ids_curr_kDif_trd = mk(ids_kline_dif[-3:])[0]
        ids_curr_kDea_trd = mk(ids_kline_dea[-3:])[0]
        ids_curr_kMacd_trd = mk(ids_kline_macd[-3:])[0]

        # 获取当前价格trend3
        self.ids_curr_low_mk = mk([self.yes2_lowest, self.yes1_lowest, self.lowest_price], 0.5)[1]
        self.ids_curr_high_mk = mk([self.yes2_highest, self.yes1_highest, self.highest_price], 0.5)[1]
        self.ids_curr_close_mk = mk([self.yes2_close, self.yes1_close, self.curr_price], 0.5)[1]

        if self.ids_curr_low_mk + self.ids_curr_high_mk + self.ids_curr_close_mk >= 2:
            self.ids_curr_trend3 = 'incs'
        elif self.ids_curr_low_mk + self.ids_curr_high_mk + self.ids_curr_close_mk <= -2:
            self.ids_curr_trend3 = 'decs'
        else:
            self.ids_curr_trend3 = 'noTrend'

        # 计算个股分时macd情况
        ids_mDif_lst, ids_mDea_lst, ids_mDw_lst = talib.MACD(np.array(self.sold_min_dct['curr_price']), 28, 68, 18)
        ids_mDif, ids_mDea, ids_mDw = ids_mDif_lst[-1], ids_mDea_lst[-1], ids_mDw_lst[-1]
        ids_mMacd_lst = (ids_mDif_lst - ids_mDea_lst) * 2
        ids_mMacd = ids_mMacd_lst[-1]
        if len(ids_mMacd_lst[-28:]) >= 3:
            ids_mMacd_trd = mk(ids_mMacd_lst[-28:])[0]
        else:
            ids_mMacd_trd = None

        res = self.trend_rec.find({'code': self.code, 'isSold': 0})
        for r in res:
            try:
                if r['money'] == 0:
                    return 0
                try:
                    curr_time_temp = str(curr_time)
                    if self.highest_price < today_highest:
                        self.highest_price = today_highest
                        self.catch_highest = pd.Series(dtype='float64')
                        self.catch_highest[curr_time_temp] = today_highest

                        self.high_point['t'] = curr_time
                        self.high_point['p'] = self.today_lowest

                        if self.from_high_bt in self.from_low_up_dct.keys():
                            self.from_high_down_dct[self.from_high_bt]['t'].clear()
                            self.from_high_down_dct[self.from_high_bt]['p'].clear()
                        else:
                            self.from_high_down_dct[self.from_high_bt] = {'t': [curr_time], 'p': [self.today_highest]}
                        self.from_high_flag = True

                    else:
                        self.catch_highest[curr_time_temp] = curr_price

                        if self.from_high_flag == True:
                            self.from_high_down_dct[self.from_high_bt]['t'].append(curr_time)
                            self.from_high_down_dct[self.from_high_bt]['p'].append(self.curr_price)
                        if self.from_high_flag == True and (self.catch_highest.values[-1] - self.catch_highest.values[0]) / self.catch_highest.values[0] < -0.008:
                            self.from_high_flag = False
                            self.from_high_bt += 1

                except Exception as e:
                    self.logger.error(e)

                if not curr_price or not today_highest:
                    return 0

                # 更新当前距离成本价之后的最高价格
                try:
                    if curr_price > r['highest_price']:
                        self.trend_rec.update_one({'code': self.code, '_id': r['_id']}, {'$set': {'highest_price': curr_price, 'highest_time': str(curr_time)}})

                        r['highest_price'] = curr_price
                    try:
                        if curr_time.hour == 14 and curr_time.minute == 59 or (curr_time.hour == 15 and curr_time.minute == 0):
                            self.trend_rec.update_one({'code': self.code, '_id': r['_id']}, {'$set': {'curr_price': curr_price}})
                    except Exception as e:
                        self.logger.error(e)

                    if curr_price == 0:
                        return 0

                except Exception as e:
                    self.logger.error(f"Sold_Error777666, e:{e}")

                # 当大于昨日成本价之后，才开始监控回落
                zhiying = 0.018
                if ('curr_macd' not in curr_dp_info['sh'].keys()) or ('curr_macd' not in curr_dp_info['sz'].keys()):
                    print(curr_dp_info)
                    return 0
                if ((curr_dp_info['sh']['curr_macd'] < 0) or (curr_dp_info['sz']['curr_macd'] < 0)) or (curr_dp_info['sh']['mk_kline_macd'] == 'decs' or curr_dp_info['sz']['mk_kline_macd'] == 'decs') or self.dapan_yes1_beili == 'top':
                    # 昨日和前一日数据对比
                    if (self.yes1_macd < self.yes2_macd and self.yes2_close > self.yes1_close and (self.trend4 == 'decs' or self.trend5 == 'decs' or self.trend6 == 'decs')):
                        zhiying = 0
                    elif (self.yes1_macd < self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'noTrend' and self.trend5 == 'noTrend' and self.trend6 == 'noTrend')) or (self.yes1_macd > self.yes2_macd and self.yes2_close > self.yes1_close and (self.trend4 == 'noTrend' and self.trend5 == 'noTrend' and self.trend6 == 'noTrend')):
                        zhiying = 0.009
                    elif (self.yes1_macd < self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'incs' or self.trend5 == 'incs' or self.trend6 == 'incs')) or (self.yes1_macd > self.yes2_macd and self.yes2_close > self.yes1_close and (self.trend4 == 'incs' or self.trend5 == 'incs' or self.trend6 == 'incs')):
                        zhiying = 0.019
                    elif (self.yes1_macd > self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'incs' and self.trend5 == 'incs' and self.trend6 == 'incs')):
                        zhiying = 0.039
                    elif (self.yes1_macd > self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'incs' or self.trend5 == 'incs' or self.trend6 == 'incs')) or (self.yes1_macd > self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 != 'decs' and self.trend5 != 'decs' and self.trend6 != 'decs')):
                        zhiying = 0.029
                    # 现在和昨日数据对比

                else:
                    # 昨日和前一日数据对比
                    if (self.yes1_macd < self.yes2_macd and self.yes2_close > self.yes1_close and (self.trend4 == 'decs' or self.trend5 == 'decs' or self.trend6 == 'decs')):
                        zhiying = 0.008
                    elif (self.yes1_macd < self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'noTrend' and self.trend5 == 'noTrend' and self.trend6 == 'noTrend')) or (self.yes1_macd > self.yes2_macd and self.yes2_close > self.yes1_close and (self.trend4 == 'noTrend' and self.trend5 == 'noTrend' and self.trend6 == 'noTrend')):
                        zhiying = 0.028
                    elif (self.yes1_macd < self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'incs' or self.trend5 == 'incs' or self.trend6 == 'incs')) or (self.yes1_macd > self.yes2_macd and self.yes2_close > self.yes1_close and (self.trend4 == 'incs' or self.trend5 == 'incs' or self.trend6 == 'incs')):
                        zhiying = 0.048
                    elif (self.yes1_macd > self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'incs' and self.trend5 == 'incs' and self.trend6 == 'incs')):
                        zhiying = 0.088
                    elif (self.yes1_macd > self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 == 'incs' or self.trend5 == 'incs' or self.trend6 == 'incs')) or (self.yes1_macd > self.yes2_macd and self.yes2_close < self.yes1_close and (self.trend4 != 'decs' and self.trend5 != 'decs' and self.trend6 != 'decs')):
                        zhiying = 0.068
                    # 现在和昨日数据对比

                sold_type = f'zy{zhiying}'
                sale_cond1 = ((r['highest_price'] - r['cost']) / r['cost'] >= zhiying)

                try:
                    # 计算当前ma5
                    # self.curr_ma5 = (self.sum_ma4 + curr_price) / 5
                    self.curr_ma6 = (self.sum_ma5 + curr_price) / 6
                    self.curr_ma7 = (self.sum_ma6 + curr_price) / 7
                    self.curr_ma8 = (self.sum_ma7 + curr_price) / 8

                    top_beili_cond1 = (self.yes2_close >= self.yes1_close or self.yes2_lowest >= self.yes1_lowest or self.yes2_highest >= self.yes1_highest or self.yes1_lowest > curr_price)
                    top_beili_cond2 = (self.yes1_ma8 > curr_price or self.yes1_ma7 > curr_price or self.yes1_ma6 > curr_price or self.yes1_ma5 > curr_price or self.yes1_ma4 > curr_price)
                    top_beili_cond3 = (self.yes1_ma7 > self.yes2_ma7 or self.yes1_ma6 > self.yes2_ma6 or self.yes1_ma8 > self.yes2_ma8 or self.yes1_ma6 < self.curr_ma6 or self.yes1_ma7 < self.curr_ma7 or self.yes1_ma8 < self.curr_ma8)
                    # 是否是假底背离，继续下跌
                    top_beili_cond4 = ((self.yes1_ma7 < self.yes2_ma7 and self.yes1_ma6 < self.yes2_ma6 and self.yes1_ma8 < self.yes2_ma8) or (self.yes1_ma6 > self.curr_ma6 and self.yes1_ma7 > self.curr_ma7 and self.yes1_ma8 > self.curr_ma8))  # # if top_beili_cond1 and top_beili_cond2 and (top_beili_cond3 or top_beili_cond4):  #     self.isAppear_top = True

                    if top_beili_cond1 and top_beili_cond2 and (top_beili_cond3 or top_beili_cond4):
                        self.isAppear_top = True

                    # self.logger.debug(f"{self.code} self.code_ma:{self.code_ma}")
                except Exception as e:
                    self.logger.error(e)

                # 判断是否出现顶背离
                try:
                    is_top_beili = (self.beili == 'top' and self.isAppear_top)

                    if (self.yes1_highest < self.yes2_highest and self.yes1_lowest < self.yes2_lowest) or (self.yes1_macd <= 0.01 and (not (self.yes1_highest > self.yes2_highest and self.yes1_lowest > self.yes2_lowest))):
                        sale_cond1 = True
                        sold_type = "xt1"

                    if ((self.yes1_highest < self.yes2_highest and self.yes1_lowest < self.yes2_lowest) or (self.yes1_macd <= 0.01 and (not (self.yes1_highest > self.yes2_highest and self.yes1_lowest > self.yes2_lowest)))) and ((self.dapan_yes1_macd <= 10 or (self.dapan_yes1_macd <= 15 and self.dapan_macd_is_get_smaller) or self.dapan_yes1_beili == 'top') or self.dapan_xiadie_trend):
                        sale_cond1 = True
                        sold_type = "qc1"

                    if (curr_price < self.yes1_lowest) and (today_highest < self.yes1_highest):
                        sale_cond1 = True
                        sold_type = "xt2"

                    if (curr_price < self.yes1_lowest) and (today_highest < self.yes1_highest) and (ids_curr_kMacd_trd == 'decs' or ids_curr_kDif_trd == 'decs' or ids_curr_kDea_trd == 'decs' or ids_curr_kDif < 0) and (ids_curr_kMacd < 0):
                        sale_cond1 = True
                        sold_type = "qc6"

                    if (curr_price < self.yes1_close) and (today_highest < self.yes1_highest) and (today_lowest < self.yes1_lowest) and (ids_curr_kMacd < 0):
                        sale_cond1 = True
                        sold_type = "qc2"

                    if (curr_price < self.yes1_close) and (ids_curr_kMacd_trd == 'decs' or ids_curr_kDif_trd == 'decs' or ids_curr_kDea_trd == 'decs' or ids_curr_kDif < 0) and (ids_curr_kMacd < 0):
                        sale_cond1 = True
                        sold_type = "qc3"

                    if (curr_price < self.yes1_lowest) and (today_highest < self.yes1_highest) and ((self.dapan_yes1_macd <= 10 or (self.dapan_yes1_macd <= 15 and self.dapan_macd_is_get_smaller) or self.dapan_yes1_beili == 'top') or self.dapan_xiadie_trend):
                        sale_cond1 = True
                        sold_type = "qc4"

                    if curr_price < self.yes1_lowest * 0.982:
                        sale_cond1 = True
                        sold_type = "qc5"

                    if curr_price < self.yes1_lowest * 0.992 and (self.yes1_highest < self.yes2_highest and self.yes1_lowest < self.yes2_lowest):
                        sale_cond1 = True
                        sold_type = "qc6"

                    # 如果大盘不好，且个股macd在减少，且昨日跌，清仓
                    if self.yes1_macd < self.yes2_macd and self.yes1_close < self.yes2_close and (self.dapan_yes1_macd <= 10 or (self.dapan_yes1_macd <= 15 and self.dapan_macd_is_get_smaller) or self.dapan_yes1_beili == 'top'):
                        sale_cond1 = True
                        sold_type = "xt3"

                    if is_top_beili:
                        sale_cond1 = True
                        sold_type = "top_beili"

                except Exception as e:
                    self.logger.error(f"Sold_Error16544, e:{e}")

                # if sale_cond1 and sale_cond2 and sale_cond3 and sale_cond4 and sale_cond5:
                if sale_cond1:

                    # 获取分时价格趋势情况
                    try:
                        price_trend = self.get_price_trend(self.sold_min_dct['curr_price'])
                        sold_trend_cond = price_trend[0][0] == 'decs'

                        sold_kline_macd_cond = not ((ids_curr_kMacd_trd == 'incs' or ids_curr_kDif_trd == 'incs' or ids_curr_kDea_trd == 'incs' or ids_curr_kDif > 0) and (ids_curr_kMacd > 0))

                    except Exception as e:
                        self.logger.error(e)

                    # self.logger.info(log_msg + '进入if')
                    # print('jk_sale:',self.catch_highest)

                    # 判断回落点,设置止盈

                    # 判断回落点数是否大于0.003
                    # if self.catch_highest.max() - self.catch_highest[-1] >= 0.003:

                    if sold_type[0:2] == 'qc' or ((sold_type[0:2] == "xt" or sold_type == "top_beili") and (curr_time.hour == 14 and curr_time.minute >= 53)):
                        if r['st'] != 0:
                            try:
                                num = r['left_num']
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                curr_yingkui = num * (curr_price - r['cost'])
                                yingkui = curr_yingkui + r['yingkui']
                                # self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui, 'yingkui': yingkui}})

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                            except Exception as e:
                                self.logger.error(f"Sold_Error2222: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, 'yingkui': yingkui, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:

                                    res = self.run_type
                                    self.logger.info(f"sold_point1: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, 'yingkui': yingkui, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui}})

                            except Exception as e:
                                self.logger.error(f"Sold_Error2: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error2', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        else:
                            try:
                                num = r['left_num']
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui = num * (curr_price - r['cost'])
                                # self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                            except Exception as e:
                                self.logger.error(f"Sold_Error33333: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                self.logger.info(f"sold_point2: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error4: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error4', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    elif (sold_type[0:2] == "xt" or sold_type == "top_beili") and ((self.catch_highest.max() - self.catch_highest[-1]) / self.catch_highest.max() >= 0.003) and sold_trend_cond:
                        if r['st'] == 0:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 8000):
                                    num = r['left_num']
                                    isSold = 1  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                                else:
                                    num = int(r['left_num'] / 300) * 200
                                    isSold = 0
                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui1 = num * (curr_price - r['cost'])
                                yingkui = yingkui1
                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error444: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error9: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error9', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 1 and (curr_price - r['soldPrice1']) / r['soldPrice1'] >= 0.003:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 8000):
                                    num = r['left_num']
                                    isSold = 1  # # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                                else:
                                    num = int(r['left_num'] / 300) * 200
                                    isSold = 0
                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui2 = num * (curr_price - r['cost'])
                                yingkui = r['yingkui1'] + yingkui2
                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error555: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})


                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error10: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error10', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 2 and (curr_price - r['soldPrice2']) / r['soldPrice2'] >= 0.003:
                            try:
                                num = r['left_num']
                                left_num = r['left_num'] - num
                                condition = {'code': r['code'], '_id': r['_id']}
                                yingkui3 = num * (curr_price - r['cost'])
                                yingkui = r['yingkui1'] + r['yingkui2'] + yingkui3

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # try:  #     # 查询剩余仓位，全部清仓  #     res_dct = self.trader.get_nums()  #     num = res_dct[k[2:]]  # except Exception as e:  #     self.logger.error(f"Sold_Error11: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  #     # # send_notice('Sold_Error11', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error8: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

                                self.logger.info(f"sold_point7: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error12: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error12', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    elif (r['highest_price'] - curr_price) / r['highest_price'] > 0.012 and curr_price > r['cost'] and sold_trend_cond and sold_kline_macd_cond:
                        if r['st'] != 0:
                            try:
                                num = r['left_num']
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                curr_yingkui = num * (curr_price - r['cost'])
                                yingkui = curr_yingkui + r['yingkui']

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                            except Exception as e:
                                self.logger.error(f"Sold_Error33: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui, 'yingkui': yingkui}})

                                self.logger.info(f"sold_point3: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error6: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error6', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                        else:
                            try:
                                num = r['left_num']
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui = num * (curr_price - r['cost'])

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                            except Exception as e:
                                self.logger.error(f"Sold_Error2408: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                self.logger.info(f"sold_point4: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error8: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error8', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    elif (r['highest_price'] - curr_price) / r['highest_price'] > 0.006 and curr_price > r['cost'] and sold_trend_cond and sold_kline_macd_cond:
                        if r['st'] == 0:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 8000):
                                    num = r['left_num']
                                    isSold = 1  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                                else:
                                    num = int(r['left_num'] / 300) * 200
                                    isSold = 0
                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui1 = num * (curr_price - r['cost'])
                                yingkui = yingkui1

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error999: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))
                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error9: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error9', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 1 and (curr_price - r['soldPrice1']) / r['soldPrice1'] >= 0.003:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 8000):
                                    num = r['left_num']
                                    isSold = 1
                                else:
                                    num = int(r['left_num'] / 300) * 200
                                    isSold = 0
                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui2 = num * (curr_price - r['cost'])
                                yingkui = r['yingkui1'] + yingkui2

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error666: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

                                self.logger.info(f"sold_point6: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")
                            except Exception as e:
                                self.logger.error(f"Sold_Error10: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error10', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 2 and (curr_price - r['soldPrice2']) / r['soldPrice2'] >= 0.003:
                            try:
                                num = r['left_num']
                                left_num = r['left_num'] - num
                                condition = {'code': r['code'], '_id': r['_id']}
                                yingkui3 = num * (curr_price - r['cost'])
                                yingkui = r['yingkui1'] + r['yingkui2'] + yingkui3

                                sale_price = curr_price - 0.03
                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                            except Exception as e:
                                self.logger.error(f"Sold_Error111: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

                                self.logger.info(f"sold_point7: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error12: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error12', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    # 如果有进来过那么，clean_flag 标记为清空
                    self.clean_flag = True

                # 顶背离清仓  # 获取最近插入的数据  # latest_rec = self.beili.find({'code': k}).sort('_id', -1).limit(1)  # latest_rec = [x for x in latest_rec]

                # cond1 = (self.yes2_close >= self.yes1_close or  #          self.yes2_lowest >= self.yes1_lowest or  #          self.yes2_highest >= self.yes1_highest)  #  # cond2 = self.yes1_ma8 > curr_price  #  # cond3 = (self.yes1_ma7 > self.yes2_ma7 or  #          self.yes1_ma6 > self.yes2_ma6 or  #          self.yes1_ma8 > self.yes2_ma8)

                # is_false_bottom = (latest_rec and latest_rec[0]['type'] == 'bottom' and latest_rec[0]['price'] * 0.99 > curr_price)  # is_top_beili = latest_rec and latest_rec[0]['type'] == 'top'  #  # if is_top_beili or is_false_bottom:  #     num = r['num']  #     condition = {'code': self.code, 'time': r['time']}  #     yingkui = num * (curr_price - r['cost'])  #     self.trend_rec.update_one(condition, {  #         '$set': {'isSold': 1, 'soldTime': str(curr_time), 'soldPrice': curr_price,  #                  'yingkui': yingkui}})  #  #       #     sale_price = curr_price - 0.03  #     res = self.trader.auto_sale_chrome(self.code, sale_price, num)  #     self.logger.info(  #         str(k) + ' 卖出, 单价:' + str(curr_price - 0.01) + ' 数量:' + str(  #             num) + ' 返回结果：' + str(res))

                # 如果有进来过那么，clean_flag 标记为清空  # self.clean_flag = True

            except Exception as e:
                # self.logger.error(e)
                raise e
                self.logger.error(f"error: {r['code']} {e}")

    async def jk_buy_pm(self, lastreq, curr_time, curr_bk_info=None, curr_dp_info=None):
        t = time.time()
        self.curr_time = curr_time
        if not lastreq:
            return 0

        # 获取基础数据情况
        # ***************************************************************************
        try:
            self.today_lowest = lastreq['today_lowest']
            self.today_highest = lastreq['today_highest']
            self.curr_price = lastreq['curr_price']
            if self.curr_price <= 4:
                return 0

            # 捕捉最小价格
            if self.lowest_price > self.today_lowest:
                self.lowest_price = self.today_lowest
                self.catch_lowest = pd.Series(dtype='float64')
                self.catch_lowest[curr_time] = self.today_lowest  # self.logger.info(f"lowest_point!!!: {self.lowest_price}")  # self.logger.debug(f"{self.code} catch_lowest:{self.catch_lowest}")
            else:
                self.catch_lowest[curr_time] = self.curr_price

            # 记录个股当日分时数据，趋势情况
            self.buy_min_dct['time'].append(str(curr_time))
            self.buy_min_dct['curr_price'].append(self.curr_price)

            if len(self.buy_min_dct['curr_price']) > 128:
                self.buy_min_dct['curr_price'].pop(0)
                self.buy_min_dct['time'].pop(0)

        except Exception as e:
            self.logger.error(f"lastreq:{lastreq}, {e}")

        # 判断个股退出条件，已经买入，则退出
        # ***************************************************************************
        # 如果第一次价格不为零，且当前价格大于了第一次买入价格，那么后面不需要再做买入操作
        if self.bt1_cost != 0:
            # 使用self.pre_price 避免频繁的更新数据库，如果上一次价格和当前价格不一样才做更新
            if self.run_type == 'trading_debug' and self.pre_price != self.curr_price:
                self.pre_price = self.curr_price
                hasBuy_res = self.hasBuy.find({'code': self.code, 'buy_date': self.today})
                for r in hasBuy_res:
                    yingkui = (self.curr_price - r['cost']) * r['num']
                    pct_bt = round(yingkui / r['money'], 4)
                    self.hasBuy.update_one({'_id': r['_id']}, {'$set': {'curr_price': self.curr_price, 'yingkui': yingkui, 'pct_bt': pct_bt}})
            return 0
        else:
            self.pre_price = 0

        # 判断当前价格是否有效
        if self.curr_price == 0:
            return 0

        # 判断是否出现顶背离 对齐：bt112_2.py
        self.top_beili_cond1 = not (self.yes2_lowest < self.yes1_lowest and self.yes2_highest < self.yes1_highest)
        self.top_beili_cond2 = (self.yes1_ma7 > self.yes2_ma7 or self.yes1_ma6 > self.yes2_ma6 or self.yes1_ma8 > self.yes2_ma8)
        self.top_beili_cond3 = (self.yes1_ma4 > self.curr_price or self.yes1_ma5 > self.curr_price or self.yes1_ma6 > self.curr_price or self.yes1_ma7 > self.curr_price)
        if self.top_beili_cond1 and self.top_beili_cond2 and self.top_beili_cond3:
            self.isAppear_top = True
            return 0

        # 如果你是底部，但是现在价格低于了这个底部，那就不能买，应该全卖出
        if self.yes1_lowest > self.curr_price:
            return 0

        try:
            # 计算个股kline macd
            # ***************************************************************************
            # 当前curr_price 结合之前 kline的macd数据
            self.ids_kline_close[-1] = self.curr_price
            ids_kline_dif, ids_kline_dea, ids_kline_dw = talib.MACD(self.ids_kline_close, 3, 8, 5)
            ids_kline_macd = (ids_kline_dif - ids_kline_dea) * 2

            ids_curr_kDif = ids_kline_dif[-1]
            ids_curr_kDea = ids_kline_dea[-1]
            ids_curr_kMacd = ids_kline_macd[-1]
            ids_curr_kMacd_trd = mk(ids_kline_macd[-3:])[0]

            if not (ids_curr_kMacd > 0 and ids_curr_kMacd_trd == 'incs'):
                return 0

            buy_cond1 = ids_curr_kMacd > 0 and ids_curr_kMacd_trd == 'incs' and ids_curr_kDif > 0
            if buy_cond1 == False:
                return 0

            # 查询是否已经买入过
            self.hasBuy_count = self.hasBuy.count_documents({'code': self.code, 'buy_date': self.today})

            if self.hasBuy_count > 0:
                print(self.code, '已经买入过，退出')
                return 0

            if buy_cond1:
                # 更新all_cangwei
                self.update_all_cangwei()

                # 因为整体仓位达到上限，导致不能交易
                if self.all_cangwei >= self.allow_all_cangwei:
                    print("self.all_cangwei >= self.allow_all_cangwei", self.all_cangwei, self.allow_all_cangwei)
                    return 0

                # 查询bt购买情况，设置购买金额
                # if (curr_dp_info['sh']['curr_macd'] < 0 and curr_dp_info['sh']['curr_dif'] < 0) or (curr_dp_info['sh']['mk_kline_macd'] == 'desc') or (self.dapan_yes1_beili == 'top'):
                #     self.money1 = self.per_top_money * 4
                #     self.money2 = self.per_top_money * 6
                #     self.money3 = 0
                #     self.money4 = 0
                #     self.money5 = 0
                # else:

                self.money1 = self.per_top_money * 3
                self.money2 = self.per_top_money * 5
                self.money3 = 0
                self.money4 = 0
                self.money5 = 0

                # 查看个股所在板块 是否已经购买 超标
                ids_bk_buy_flag = False
                for bk_code in self.ids_selfBk_lst:
                    # print(bk_code, self.ids_selfBk_lst)
                    bk_ids_r = self.bk_ids.find_one({'date': self.today, 'bk_code': bk_code})
                    # print('bk_ids_r', bk_ids_r)
                    if bk_ids_r:

                        if bk_ids_r['hasBuy_nums'] < bk_ids_r['idsNums_allowed_buy'] and self.code not in bk_ids_r['hasBuy_lst']:
                            ids_bk_buy_flag = True
                            new_hasBuy_lst = bk_ids_r['hasBuy_lst'] + [self.code]
                            new_hasBuy_nums = bk_ids_r['hasBuy_nums'] + 1
                            if (curr_time.hour == 14 and curr_time.minute > 45):
                                self.bk_ids.update_one({'date': self.today, 'bk_code': bk_code}, {'$set': {'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums}})
                                print('bk_ids update:', {'code:': self.code, 'bk_code': bk_code, 'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums})
                            else:
                                print('bk_ids will update:', {'code:': self.code, 'bk_code': bk_code, 'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums})

                if ids_bk_buy_flag == False:
                    # print(self.code, 'return in ids_bk_buy_flag:', ids_bk_buy_flag)
                    return 0

                try:
                    self.money = self.money1

                    # 计算成本单价
                    self.num = (round(self.money / self.curr_price / 100) * 100)

                    if self.num == 0 and self.money != 0:
                        self.num = 100
                        self.buyMoney = self.num * self.curr_price
                        self.cost = (self.buyMoney + 20) / self.num
                    elif self.num == 0 and self.money == 0:
                        self.buyMoney = 0
                        self.cost = 0
                    else:
                        self.buyMoney = self.num * self.curr_price
                        self.cost = (self.buyMoney + 20) / self.num

                    self.bt = 1

                    # 更新仓位情况
                    self.all_cangwei += self.buyMoney

                    # self.autoBuy(ndict, money, 'sale1')
                    self.buy_price = self.curr_price + 0.01
                    print(f"{curr_time}:{self.code}满足条件，将在尾盘买入, cond1:{buy_cond1}, kMacd:{ids_curr_kMacd}, kMacd_trd:{ids_curr_kMacd_trd}, kDif:{ids_curr_kDif}")
                    if self.money != 0 and (curr_time.hour == 14 and curr_time.minute > 45):
                        if self.run_type == 'trading':
                            with self.trade_lock:
                                res = self.trader.auto_buy_session(self.code, self.buy_price, self.num)
                                try:
                                    res_dct = json.loads(str(res))
                                    if res_dct['Status'] == 0:
                                        self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})
                                except Exception as e:
                                    self.logger.error(str(e) + ' 返回结果：' + str(res))
                        elif self.run_type in ['trading_debug', 'backtest']:
                            res = 'backtest'
                            self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0, 'trd_buy_type': self.trade_buy_type})

                        self.logger.info(self.code + ' bt1买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))

                        self.bt1_cost = self.cost

                    self.catch_lowest = pd.Series(dtype='float64')

                except Exception as e:
                    # raise e
                    self.logger.error(f"error: {self.code}, {e}")

        except Exception as e:
            # raise e
            self.logger.error(self.code + ' error :' + str(e))

    async def jk_sale_pm(self, lastreq, curr_time, curr_bk_info=None, curr_dp_info=None):
        self.curr_time = curr_time
        if not lastreq:
            return 0

        try:
            today_highest = lastreq['today_highest']
            today_lowest = lastreq['today_lowest']
            curr_price = lastreq['curr_price']
            curr_pct_chg = lastreq['pct_chg']
        except Exception as e:
            self.logger.error(f"lastreq:{lastreq}, {e}")

        self.sold_min_dct['time'].append(str(curr_time))
        self.sold_min_dct['curr_price'].append(curr_price)

        if len(self.sold_min_dct['curr_price']) > 128:
            self.sold_min_dct['curr_price'].pop(0)
            self.sold_min_dct['time'].pop(0)

        # 判断当前价格是否有效
        if curr_price == 0:
            return 0

        # 当前curr_price 结合之前 kline的macd数据
        self.ids_kline_close[-1] = curr_price
        ids_kline_dif, ids_kline_dea, ids_kline_dw = talib.MACD(self.ids_kline_close, 3, 8, 5)
        ids_kline_macd = (ids_kline_dif - ids_kline_dea) * 2

        ids_curr_kDif = ids_kline_dif[-1]
        ids_curr_kDea = ids_kline_dea[-1]
        ids_curr_kMacd = ids_kline_macd[-1]
        ids_curr_kDif_trd = mk(ids_kline_dif[-3:])[0]
        ids_curr_kDea_trd = mk(ids_kline_dea[-3:])[0]
        ids_curr_kMacd_trd = mk(ids_kline_macd[-3:])[0]

        # 计算个股分时macd情况
        ids_mDif_lst, ids_mDea_lst, ids_mDw_lst = talib.MACD(np.array(self.sold_min_dct['curr_price']), 28, 68, 18)
        ids_mDif, ids_mDea, ids_mDw = ids_mDif_lst[-1], ids_mDea_lst[-1], ids_mDw_lst[-1]
        ids_mMacd_lst = (ids_mDif_lst - ids_mDea_lst) * 2
        ids_mMacd = ids_mMacd_lst[-1]
        if len(ids_mMacd_lst[-28:]) >= 3:
            ids_mMacd_trd = mk(ids_mMacd_lst[-28:])[0]
        else:
            ids_mMacd_trd = None

        res = self.trend_rec.find({'code': self.code, 'isSold': 0})
        for r in res:
            try:
                if r['money'] == 0:
                    return 0
                try:
                    curr_time_temp = str(curr_time)
                    if self.highest_price < today_highest:
                        self.highest_price = today_highest
                        self.catch_highest = pd.Series(dtype='float64')
                        self.catch_highest[curr_time_temp] = today_highest
                    else:
                        self.catch_highest[curr_time_temp] = curr_price
                except Exception as e:
                    self.logger.error(e)

                if not curr_price or not today_highest:
                    return 0

                # 更新当前距离成本价之后的最高价格
                try:
                    if curr_price > r['highest_price']:
                        self.trend_rec.update_one({'code': self.code, '_id': r['_id']}, {'$set': {'highest_price': curr_price, 'highest_time': str(curr_time)}})
                        r['highest_price'] = curr_price
                    try:
                        if curr_time.hour == 14 and curr_time.minute == 59 or (curr_time.hour == 15 and curr_time.minute == 0):
                            self.trend_rec.update_one({'code': self.code, '_id': r['_id']}, {'$set': {'curr_price': curr_price}})
                    except Exception as e:
                        self.logger.error(e)

                    if curr_price == 0:
                        return 0

                except Exception as e:
                    self.logger.error(f"Sold_Error777666, e:{e}")

                # 当大于昨日成本价之后，才开始监控回落
                zhiying = 0.018
                if ('curr_macd' not in curr_dp_info['sh'].keys()) or ('curr_macd' not in curr_dp_info['sh'].keys()):
                    print(curr_dp_info)
                    return 0
                if ((curr_dp_info['sh']['curr_macd'] < 0) or (curr_dp_info['sz']['curr_macd'] < 0)) or (curr_dp_info['sh']['mk_kline_macd'] == 'decs' or curr_dp_info['sz']['mk_kline_macd'] == 'decs') or self.dapan_yes1_beili == 'top':
                    zhiying = 0.012
                else:
                    zhiying = 0.018
                sold_type = f'zy{zhiying}'

                try:
                    # 计算当前ma5
                    # self.curr_ma5 = (self.sum_ma4 + curr_price) / 5
                    self.curr_ma6 = (self.sum_ma5 + curr_price) / 6
                    self.curr_ma7 = (self.sum_ma6 + curr_price) / 7
                    self.curr_ma8 = (self.sum_ma7 + curr_price) / 8

                    top_beili_cond1 = (self.yes2_close >= self.yes1_close or self.yes2_lowest >= self.yes1_lowest or self.yes2_highest >= self.yes1_highest or self.yes1_lowest > curr_price)
                    top_beili_cond2 = (self.yes1_ma8 > curr_price or self.yes1_ma7 > curr_price or self.yes1_ma6 > curr_price or self.yes1_ma5 > curr_price or self.yes1_ma4 > curr_price)
                    top_beili_cond3 = (self.yes1_ma7 > self.yes2_ma7 or self.yes1_ma6 > self.yes2_ma6 or self.yes1_ma8 > self.yes2_ma8 or self.yes1_ma6 < self.curr_ma6 or self.yes1_ma7 < self.curr_ma7 or self.yes1_ma8 < self.curr_ma8)
                    # 是否是假底背离，继续下跌
                    top_beili_cond4 = ((self.yes1_ma7 < self.yes2_ma7 and self.yes1_ma6 < self.yes2_ma6 and self.yes1_ma8 < self.yes2_ma8) or (self.yes1_ma6 > self.curr_ma6 and self.yes1_ma7 > self.curr_ma7 and self.yes1_ma8 > self.curr_ma8))  # # if top_beili_cond1 and top_beili_cond2 and (top_beili_cond3 or top_beili_cond4):  #     self.isAppear_top = True

                    if top_beili_cond1 and top_beili_cond2 and (top_beili_cond3 or top_beili_cond4):
                        self.isAppear_top = True

                    # self.logger.debug(f"{self.code} self.code_ma:{self.code_ma}")
                except Exception as e:
                    self.logger.error(e)

                # 判断是否出现顶背离
                try:
                    is_top_beili = (self.beili == 'top' and self.isAppear_top)

                    if (self.yes1_highest < self.yes2_highest and self.yes1_lowest < self.yes2_lowest) or (self.yes1_macd <= 0.01 and (not (self.yes1_highest > self.yes2_highest and self.yes1_lowest > self.yes2_lowest))):
                        zhiying = -10
                        sold_type = f'xt1{zhiying}'

                    if ((self.yes1_highest < self.yes2_highest and self.yes1_lowest < self.yes2_lowest) or (self.yes1_macd <= 0.01 and (not (self.yes1_highest > self.yes2_highest and self.yes1_lowest > self.yes2_lowest)))) and ((self.dapan_yes1_macd <= 10 or (self.dapan_yes1_macd <= 15 and self.dapan_macd_is_get_smaller) or self.dapan_yes1_beili == 'top') or self.dapan_xiadie_trend):
                        zhiying = -10
                        sold_type = f'qc1{zhiying}'

                    if (curr_price < self.yes1_lowest) and (today_highest < self.yes1_highest):
                        zhiying = -10
                        sold_type = f'xt2{zhiying}'

                    if (curr_price < self.yes1_lowest) and (today_highest < self.yes1_highest) and (ids_curr_kMacd_trd == 'decs' or ids_curr_kDif_trd == 'decs' or ids_curr_kDea_trd == 'decs' or ids_curr_kDif < 0) and (ids_curr_kMacd < 0):
                        zhiying = -10
                        sold_type = f'qc2{zhiying}'

                    if (curr_price < self.yes1_close) and (today_highest < self.yes1_highest) and (today_lowest < self.yes1_lowest) and (ids_curr_kMacd < 0):
                        zhiying = -10
                        sold_type = f'qc3{zhiying}'

                    if (curr_price < self.yes1_close) and (ids_curr_kMacd_trd == 'decs' or ids_curr_kDif_trd == 'decs' or ids_curr_kDea_trd == 'decs' or ids_curr_kDif < 0) and (ids_curr_kMacd < 0):
                        zhiying = -10
                        sold_type = f'qc4{zhiying}'

                    if (curr_price < self.yes1_lowest) and (today_highest < self.yes1_highest) and ((self.dapan_yes1_macd <= 10 or (self.dapan_yes1_macd <= 15 and self.dapan_macd_is_get_smaller) or self.dapan_yes1_beili == 'top') or self.dapan_xiadie_trend):
                        zhiying = -10
                        sold_type = f'qc5{zhiying}'

                    if curr_price < self.yes1_lowest * 0.982:
                        zhiying = -10
                        sold_type = f'qc6{zhiying}'

                    if curr_price < self.yes1_lowest * 0.992 and (self.yes1_highest < self.yes2_highest and self.yes1_lowest < self.yes2_lowest):
                        zhiying = -10
                        sold_type = f'qc7{zhiying}'

                    # 如果大盘不好，且个股macd在减少，且昨日跌，清仓
                    if self.yes1_macd < self.yes2_macd and self.yes1_close < self.yes2_close and (self.dapan_yes1_macd <= 10 or (self.dapan_yes1_macd <= 15 and self.dapan_macd_is_get_smaller) or self.dapan_yes1_beili == 'top'):
                        zhiying = -10
                        sold_type = f'xt3{zhiying}'

                    if is_top_beili:
                        zhiying = -10
                        sold_type = f'qc8{zhiying}'

                    # 获取板块情况, 根据板块情况来确定是否清仓卖出  #  # self.bk_code_lst = self.bk_code[self.bk_code['code'] == self.code]['bk_code'].values  # for bk_code in self.bk_code_lst:  #     if bk_code in curr_bk_info.keys():  #  #         if (curr_bk_info[bk_code]['curr_kMacd'] < 0 or curr_bk_info[bk_code]['curr_kMacd_trend'] == 'decs'):  #             zhiying = -10  #             sold_type = f'qc_bk{zhiying}'

                except Exception as e:
                    self.logger.error(f"Sold_Error16544, e:{e}")

                sale_cond1 = ((r['highest_price'] - r['cost']) / r['cost'] >= zhiying)

                print(f"{self.code}, sale_cond1:{sale_cond1}, sold_type:{sold_type}, ids_mMacd_trd:{ids_mMacd_trd}")

                if sale_cond1 or (curr_time.hour == 14 and curr_time.minute >= 43):

                    # 获取分时价格趋势情况
                    try:

                        # 开盘macd还未计算出来时，使用price_trend
                        price_trend = ''
                        # if curr_time.hour == 9 and curr_time.minute <= 45:
                        if len(self.sold_min_dct['curr_price']) >= 28:
                            price_trend = mk(self.sold_min_dct['curr_price'])[0]

                        # price_trend = self.get_price_trend(self.sold_min_dct['curr_price'])
                        # sold_trend_cond = (price_trend[0][0] == 'decs')

                        sold_kline_macd_cond = not ((ids_curr_kMacd_trd == 'incs' or ids_curr_kDif_trd == 'incs' or ids_curr_kDea_trd == 'incs' or ids_curr_kDif > 0) and (ids_curr_kMacd > 0))

                    except Exception as e:
                        self.logger.error(e)

                    # print(self.code, "(r['highest_price'] - curr_price) / r['highest_price'] > 0.00368:", (r['highest_price'] - curr_price) / r['highest_price'] > 0.00368)
                    # print(self.code, "price_trend ", price_trend)
                    # print(self.code, "ids_mMacd_trd ", ids_mMacd_trd)
                    # print(self.code, "np.isnan(ids_mMacd) ", np.isnan(ids_mMacd))
                    if curr_time.hour == 14 and curr_time.minute >= 43:
                        if curr_pct_chg >= 9.68:
                            continue
                        if r['st'] != 0:
                            try:
                                num = r['left_num']
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                curr_yingkui = num * (curr_price - r['cost'])
                                yingkui = curr_yingkui + r['yingkui']
                                # self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui, 'yingkui': yingkui}})

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                            except Exception as e:
                                self.logger.error(f"Sold_Error2222: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, 'yingkui': yingkui, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:

                                    res = self.run_type
                                    self.logger.info(f"sold_point1: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, 'yingkui': yingkui, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui}})

                            except Exception as e:
                                self.logger.error(f"Sold_Error2: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error2', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        else:
                            try:
                                num = r['left_num']
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui = num * (curr_price - r['cost'])
                                # self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                            except Exception as e:
                                self.logger.error(f"Sold_Error33333: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                self.logger.info(f"sold_point2: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error4: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error4', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    elif ((r['highest_price'] - curr_price) / r['highest_price'] > 0.00368 and ((price_trend == "decs" and ids_mMacd_trd == 'decs') or (price_trend == "decs" and np.isnan(ids_mMacd) and (curr_time.hour == 9 and curr_time.minute <= 40)))) or sold_type[0:2] == 'qc':

                        if sold_type[0:2] == 'qc':
                            try:
                                num = r['left_num']
                                isSold = 1  # # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]

                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui2 = num * (curr_price - r['cost'])
                                yingkui = r['yingkui1'] + yingkui2
                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error555: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})


                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error10: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error10', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 0:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 8000):
                                    num = r['left_num']
                                    isSold = 1  # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]
                                else:
                                    num = int(r['left_num'] / 300) * 200
                                    isSold = 0
                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui1 = num * (curr_price - r['cost'])
                                yingkui = yingkui1
                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error444: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")
                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error9: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error9', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 1 and (curr_price - r['soldPrice1']) / r['soldPrice1'] >= 0.003:
                            try:
                                num = r['left_num']
                                isSold = 1  # # 查询剩余仓位，全部清仓  # res_dct = self.trader.get_nums()  # num = res_dct[k[2:]]

                                left_num = r['left_num'] - num
                                condition = {'code': self.code, 'time': r['time'], '_id': r['_id']}
                                yingkui2 = num * (curr_price - r['cost'])
                                yingkui = r['yingkui1'] + yingkui2
                                # 更新仓位情况
                                self.all_cangwei -= num * curr_price

                                sale_price = curr_price - 0.03
                            except Exception as e:
                                self.logger.error(f"Sold_Error555: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                            try:
                                is_sale_success = True
                                if self.run_type == 'trading':
                                    with self.trade_lock:
                                        try:
                                            res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            res_dct = json.loads(str(res))
                                            if res_dct['Status'] != 0 and '可用股份数不足' in str(res):
                                                await asyncio.sleep(0.68)
                                                curr_reality_cangwei = self.trend_reality.find_one({'code': self.code, 'curr_time': str(curr_time)})
                                                if curr_reality_cangwei:
                                                    num = curr_reality_cangwei['uNum']
                                                else:
                                                    reality_cangwei = selfcheck.check_cangwei(self.trader).get_reality_cangwei()
                                                    num = reality_cangwei[self.code]['uNum']
                                                    self.update_trend_reality(reality_cangwei, curr_time)
                                                    await asyncio.sleep(0.68)
                                                res = self.trader.auto_sale_session(self.code, sale_price, num)
                                            elif res_dct['Status'] != 0:
                                                is_sale_success = False
                                        except Exception as e:
                                            is_sale_success = False
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})


                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {'$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error10: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error10', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    # 如果有进来过那么，clean_flag 标记为清空
                    self.clean_flag = True

            except Exception as e:
                # self.logger.error(e)
                # raise e
                self.logger.error(f"error: {r['code']} {e}")

    def get_bts(self):
        # 初始化查询当日bt买入情况, 在jk_buy中，新增一笔买入，那么更新self.bt1_nums 和 self.bt1_codes_lst
        self.bt1_nums = self.hasBuy.count_documents({'bt': 1, 'buy_date': self.today})
        self.bt2_nums = self.hasBuy.count_documents({'bt': 2, 'buy_date': self.today})
        self.bt3_nums = self.hasBuy.count_documents({'bt': 3, 'buy_date': self.today})
        # self.bt4_nums = self.hasBuy.count_documents({'bt': 4, 'buy_date': self.today})
        # self.bt5_nums = self.hasBuy.count_documents({'bt': 5, 'buy_date': self.today})
        self.bt1_lst = self.hasBuy.find({'bt': 1, 'buy_date': self.today})
        self.bt2_lst = self.hasBuy.find({'bt': 2, 'buy_date': self.today})
        self.bt3_lst = self.hasBuy.find({'bt': 3, 'buy_date': self.today})
        # self.bt4_lst = self.hasBuy.find({'bt': 4, 'buy_date': self.today})
        self.bt1_codes_lst = []
        self.bt2_codes_lst = []
        self.bt3_codes_lst = []
        self.bt4_codes_lst = []
        for b in self.bt1_lst:
            self.bt1_codes_lst.append(b['code'])
        for b in self.bt2_lst:
            self.bt2_codes_lst.append(b['code'])
        for c in self.bt3_lst:
            self.bt3_codes_lst.append(c['code'])

        bt1_lst = self.hasBuy.find({'bt': 1, 'buy_date': self.today})
        self.first_buy_nums = 0
        self.first_buy_lst = []
        for b in bt1_lst:
            if b['money'] == 0:
                res = self.hasBuy.find_one({'code': b['code'], 'bt': 2, 'buy_date': self.today})
                if res:
                    self.first_buy_nums += 1
                    self.first_buy_lst.append(b['code'])
            else:
                self.first_buy_nums += 1
                self.first_buy_lst.append(b['code'])

    def update_all_cangwei(self):
        # 设定所有股票总仓位, 在后面交易过程中self.all_cangwei，买入加，卖出减
        try:
            # 若果仓位超标，退出交易
            self.all_nosale_trend_rec = self.trend_rec.find({'isSold': 0})
            self.today_all_hasBuy = self.hasBuy.find({'buy_date': self.today})
            self.all_cangwei = 0
            for a in self.all_nosale_trend_rec:
                self.all_cangwei += a['left_num'] * a['cost']
            for t in self.today_all_hasBuy:
                self.all_cangwei += t['money']

            # self.logger.debug(f"{self.code} self.all_cangwei:{self.all_cangwei}, self.per_top_money:{self.per_top_money}")

        except Exception as e:
            self.logger.error(e)

    def update_trend_reality(self, reality_cangwei, curr_time):
        self.trend_reality.drop()
        for value in reality_cangwei.values():
            value['curr_time'] = str(curr_time)
            self.trend_reality.insert(value)

    def get_price_trend(self, curr_price_lst):
        if np.isnan(curr_price_lst[-30:]).sum() == 0 and len(curr_price_lst[-30:]) >= 3:
            min1_price_trend = mk(curr_price_lst[-30:], 0.5)
        else:
            min1_price_trend = (None, 0)

        if np.isnan(curr_price_lst[-60:]).sum() == 0 and len(curr_price_lst[-60:]) >= 3:
            min3_price_trend = mk(curr_price_lst[-60:], 0.5)
        else:
            min3_price_trend = (None, 0)

        if np.isnan(curr_price_lst[-90:]).sum() == 0 and len(curr_price_lst[-120:]) >= 3:
            min6_price_trend = mk(curr_price_lst[-120:], 0.5)
        else:
            min6_price_trend = (None, 0)
        return (min1_price_trend, min3_price_trend, min6_price_trend)

    def get_macd_trend(self, macd_lst):
        # print('macd_lst[-17] len:', len(macd_lst[-17:]), ' nan.sum:', np.isnan(macd_lst[-17:]).sum(), ' not nan:', len(macd_lst[-17:])-np.isnan(macd_lst[-17:]).sum())
        if np.isnan(macd_lst[-30:]).sum() == 0 and len(macd_lst[-30:]) >= 3:
            min1_macd_trend = mk(macd_lst[-30:], 0.5)
        else:
            min1_macd_trend = (None, 0)
        if np.isnan(macd_lst[-60:]).sum() == 0 and len(macd_lst[-60:]) >= 3:
            min3_macd_trend = mk(macd_lst[-60:], 0.5)
        else:
            min3_macd_trend = (None, 0)
        if np.isnan(macd_lst[-90:]).sum() == 0 and len(macd_lst[-120:]) >= 3:
            min6_macd_trend = mk(macd_lst[-120:], 0.5)
        else:
            min6_macd_trend = (None, 0)
        return (min1_macd_trend, min3_macd_trend, min6_macd_trend)

    def get_dif_trend(self, dif_lst):
        if np.isnan(dif_lst[-30:]).sum() == 0 and len(dif_lst[-30:]) >= 3:
            min1_dif_trend = mk(dif_lst[-30:], 0.5)
        else:
            min1_dif_trend = (None, 0)
        if np.isnan(dif_lst[-60:]).sum() == 0 and len(dif_lst[-60:]) >= 3:
            min3_dif_trend = mk(dif_lst[-60:], 0.5)
        else:
            min3_dif_trend = (None, 0)
        if np.isnan(dif_lst[-90:]).sum() == 0 and len(dif_lst[-120:]) >= 3:
            min6_dif_trend = mk(dif_lst[-120:], 0.5)
        else:
            min6_dif_trend = (None, 0)
        return (min1_dif_trend, min3_dif_trend, min6_dif_trend)

    def get_dea_trend(self, dea_lst):
        if np.isnan(dea_lst[-30:]).sum() == 0 and len(dea_lst[-30:]) >= 3:
            min1_dea_trend = mk(dea_lst[-30:], 0.5)
        else:
            min1_dea_trend = (None, 0)
        if np.isnan(dea_lst[-60:]).sum() == 0 and len(dea_lst[-60:]) >= 3:
            min3_dea_trend = mk(dea_lst[-60:], 0.5)
        else:
            min3_dea_trend = (None, 0)
        if np.isnan(dea_lst[-90:]).sum() == 0 and len(dea_lst[-120:]) >= 3:
            min6_dea_trend = mk(dea_lst[-120:], 0.5)
        else:
            min6_dea_trend = (None, 0)
        return (min1_dea_trend, min3_dea_trend, min6_dea_trend)

    def get_macd(self, data):
        # t = time.time()
        # code_df = pd.DataFrame()
        code_macd_dct = {}
        # data = data[~np.isnan(data['curr_price'])]
        # code_df.fillna(value=0, inplace=True)
        code_macd_dct['time'] = data['time']
        code_macd_dct['curr_price'] = data['curr_price']
        # code_df['dif'], code_df['dea'], code_df['macd'] = talib.MACD(data['curr_price'], 12, 26, 9)

        code_macd_dct['dif'], code_macd_dct['dea'], code_macd_dct['macd'] = talib.MACD(np.array(data['curr_price']), 28, 68, 18)
        # code_df['dif'], code_df['dea'], code_df['macd'] = talib.MACD(data['curr_price'], 60, 140, 40)
        # code_macd_dct['macd'] = code_macd_dct['macd'] * 2
        # print('time:', time.time()-t)
        # print(code_macd_dct)
        return code_macd_dct


class Trend:
    def __init__(self, today, yestoday, run_type):

        self.run_type = run_type

        # 设置交易时间
        self.today = today
        self.yestoday = yestoday

        # 获取当前执行文件的名称
        self.file_name = str(os.path.basename(__file__).split('.')[0])

        # 设置交易数据源路径
        if self.run_type == 'trading':
            self.data_path = f'./find_trend/k_daily{self.yestoday}'
        elif self.run_type == 'trading_debug':
            self.data_path = f'./find_trend/k_daily{self.yestoday}'
        elif self.run_type == 'backtest':
            self.data_path = f'./find_trend/backtest_k_daily4'

        # 连接mongoDB
        self.myclient = pm.MongoClient("mongodb://localhost:27017/")
        self.fd = self.myclient["freedom"]
        if self.run_type == 'backtest':
            self.trend_rec = self.fd[self.file_name + 'trend_rec']
            self.hasBuy = self.fd[self.file_name + 'trend_has_buy']
            self.basic_data_store = self.fd[self.file_name + 'trend__basic_data']
            self.trend_reality = self.fd[self.file_name + 'trend_reality']
            self.bk_ids = self.fd[self.file_name + '_bk_ids']
        elif self.run_type == 'trading':
            self.trend_rec = self.fd['trend_rec']
            self.hasBuy = self.fd['trend_has_buy']
            self.basic_data_store = self.fd['trend__basic_data']
            self.trend_reality = self.fd['trend_reality']
            self.bk_ids = self.fd['trend_bk_ids']
            self.trend_basic_data = self.fd['trend_basic_data_' + self.today]

        elif self.run_type == 'trading_debug':
            self.trend_rec = self.fd[self.file_name + '_trd_rec']
            self.hasBuy = self.fd[self.file_name + '_trd_has_buy']
            self.basic_data_store = self.fd[self.file_name + '_trd__basic_data']
            self.trend_reality = self.fd[self.file_name + '_trd_reality']
            self.bk_curr_rec = self.fd[self.file_name + '_bk_curr_rec' + self.today]
            self.bk_ids = self.fd[self.file_name + '_bk_ids']
            # self.trend_basic_data = self.fd['trend_basic_simplify_data_' + self.today]
            self.trend_basic_data = self.fd['trend_basic_data_' + self.today]

        # 启动日志
        # self.logger = Logger('./trading_' + str(self.today) + '.log').get_logger()
        if self.run_type == 'trading':
            self.logger_path = f"{self.data_path}/log_trd"
            if not os.path.exists(self.logger_path):
                os.mkdir(self.logger_path)

        elif self.run_type == 'trading_debug':
            self.logger_path = f"{self.data_path}/log_{self.file_name}"
            if not os.path.exists(self.logger_path):
                os.mkdir(self.logger_path)

        self.logger = self.get_trend_logger()

        # 导入交易模块
        if self.run_type == 'trading':
            # 执行自检， 创建selenium句柄
            try:
                self.trader = Auto_trade(True)
                self.selfcheck = selfcheck.check_cangwei(self.trader)
                self.selfcheck.update_mongo()
            except Exception as e:
                self.logger.error(e)

        # 设置ip池
        self.ipPool = self.fd['ipPool']
        self.allIpPool = self.fd['allIpPool']

        # 设置板块涨跌幅变化临时存储的列表
        self.bk_chg_pct_dct = {}
        self.bk_chg_green_dct = {}
        self.bk_min_price_dct = {}

        self.curr_bk_info = {}
        self.curr_dp_info = {'sh': {}, 'sz': {}}
        self.bk_kline = {}
        self.bk_close_lst = {}

        self.dp_chg_pct_dct = {'sh': [], 'sz': []}
        self.dp_min_price_dct = {'sh': [], 'sz': [], 'time': []}

        # 设置url获取bk，dp, ids 结果的res变量
        self.bk_url_res = ''
        self.gn_url_res = ''
        self.dp_url_res = ''
        self.ids_url_res = None

        self.bk_name = pd.read_excel('./bk_info/bk_name.xlsx')
        self.bk_code = pd.read_excel('./bk_info/bk_code.xlsx')

        for per in self.bk_name.itertuples():
            try:
                # 获取后，直接在此一次性颠倒数据
                if os.path.exists(f'{self.data_path}/bk_kline/{per.code}.xlsx'):
                    self.bk_kline[per.code] = pd.read_excel(f'{self.data_path}/bk_kline/{per.code}.xlsx').iloc[::-1]
                    self.bk_close_lst[per.code] = copy.deepcopy(self.bk_kline[per.code]['close'].values)
                self.bk_chg_pct_dct[per.code] = []
                self.bk_chg_green_dct[per.code] = []
                self.bk_min_price_dct[per.code] = []

            except Exception as e:
                self.logger.error(e)

        # 获取sh和sz指数数据
        sz_df = pd.read_excel(f'{self.data_path}/399001.xlsx')
        sh_df = pd.read_excel(f'{self.data_path}/000001.xlsx')

        self.sz_price_lst = sz_df.iloc[::-1]['close']
        self.sh_price_lst = sh_df.iloc[::-1]['close']
        self.sz_macd_lst = sz_df.iloc[::-1]['yes1_macd']
        self.sh_macd_lst = sh_df.iloc[::-1]['yes1_macd']

        # 在这里，上面即便列表颠倒后，但是索引也会跟着颠倒，例如100-0，所以[index]取值的时候，还是依照索引来
        self.sz_price_lst = np.append(self.sz_price_lst, self.sz_price_lst[0])
        self.sh_price_lst = np.append(self.sh_price_lst, self.sh_price_lst[0])

        self.sz_macd_lst = np.append(self.sz_macd_lst, self.sz_macd_lst[0])
        self.sh_macd_lst = np.append(self.sh_macd_lst, self.sh_macd_lst[0])

        self.sz_yestoday = sz_df[sz_df['trade_date'] == self.yestoday]
        self.sh_yestoday = sh_df[sh_df['trade_date'] == self.yestoday]

        # 判断大盘情况，选择交易形式 trade_am or trade_pm
        if (((self.sz_yestoday.loc[0, 'trend3'] == 'incs' and self.sh_yestoday.loc[0, 'trend3'] == 'incs') or (self.sz_yestoday.loc[0, 'trend4'] == 'incs' and self.sh_yestoday.loc[0, 'trend4'] == 'incs')) and (self.sz_yestoday.loc[0, 'macd_trend3'] != 'decs' and self.sh_yestoday.loc[0, 'macd_trend3'] != 'decs' and self.sz_yestoday.loc[0, 'macd_trend4'] != 'decs' and self.sh_yestoday.loc[0, 'macd_trend4'] != 'decs')) or (
                ((self.sz_yestoday.loc[0, 'macd_trend3'] == 'incs' and self.sh_yestoday.loc[0, 'macd_trend3'] == 'incs') or (self.sz_yestoday.loc[0, 'macd_trend4'] == 'incs' and self.sh_yestoday.loc[0, 'macd_trend4'] == 'incs')) and (self.sz_yestoday.loc[0, 'trend3'] != 'decs' and self.sh_yestoday.loc[0, 'trend3'] != 'decs' and self.sz_yestoday.loc[0, 'trend4'] != 'decs' and self.sh_yestoday.loc[0, 'trend4'] != 'decs')) or (
                self.sz_yestoday.loc[0, 'beili'] == 'bottom' and self.sh_yestoday.loc[0, 'beili'] == 'bottom'):
            self.trade_buy_type = 'am'
            self.trade_sale_type = 'am'
            print('Trade buy type is am')
            print('Trade sold type is am')
        else:
            self.trade_buy_type = 'am'
            self.trade_sale_type = 'am'
            print('Trade buy type is am')
            print('Trade sold type is am')

        # 获取 当日 jk_ids 代码
        self.all_trend_df = pd.read_excel(f'{self.data_path}/000trend_k_list.xlsx')
        if self.trade_buy_type == 'am':
            # self.all_jk_buy_df = pd.read_excel(f'{self.data_path}/jk_ids.xlsx')
            self.all_jk_buy_df = pd.read_excel(f'{self.data_path}/jk_ids_all.xlsx')
        elif self.trade_buy_type == 'pm':
            self.all_jk_buy_df = pd.read_excel(f'{self.data_path}/jk_ids_all.xlsx')

        self.all_jk_list = [self.chg_code_type(str(code).zfill(6)) for code in self.all_jk_buy_df['ts_code'].values]
        # 获取当日筛选出来的趋势股数量，作为当日的行情指标
        self.trend_nums = len(self.all_jk_list)
        self.all_jk_buy_list = copy.deepcopy(self.all_jk_list)
        self.all_jk_sale_list = []

        # 加入未卖出标的
        for r in self.trend_rec.find({'isSold': 0}):
            # print(r['code'])
            self.all_jk_sale_list.append(r['code'])
            self.all_jk_list.append(r['code'])  # print(222,self.all_jk_list)  # if r['code'] in self.all_jk_buy_list:  #     self.all_jk_buy_list.remove(r['code'])

        # all_jk_sale_list 可能有多日未卖出，需要去重
        self.all_jk_sale_list = set(self.all_jk_sale_list)

        # self.all_jk_list = set(self.all_jk_list)
        # 合并新的买和卖标的
        self.all_jk_list = set(list(self.all_jk_buy_list) + list(self.all_jk_sale_list))

        # 获取当日 jk_bk 代码
        self.bk_jk_df = pd.read_excel(f'{self.data_path}/jk_bk.xlsx')
        self.bk_jk_detail = pd.read_excel(f'{self.data_path}/jk_bk_detail.xlsx')
        self.bk_jk_lst = self.bk_jk_df['ts_code'].values
        # self.bk_jk_lst = self.bk_name['code'].values
        # 获取当日bk所含ids 数量，ids代码，以及ids具体数据
        self.bk_ids_1v1_df = pd.read_excel(f'{self.data_path}/bk_ids_1v1.xlsx')
        self.bk_ids_dct = {}
        hasBuy_res = self.hasBuy.find({'buy_date': self.today})

        print('处理 bk_ids  数据开始')
        for per_bk in self.bk_name.itertuples():
            bk_ids_lst = [self.chg_code_type(str(c).zfill(6)) for c in self.bk_ids_1v1_df[self.bk_ids_1v1_df['bk_code'] == per_bk.code]['ts_code'].values]
            bk_ids_nums = len(bk_ids_lst)

            # ratio = 6/len(self.all_jk_buy_list)
            # idsNums_allowed_buy = int(bk_ids_nums * ratio)
            # if bk_ids_nums != 0 and idsNums_allowed_buy == 0:
            #     idsNums_allowed_buy = 1
            # elif bk_ids_nums == 0 and idsNums_allowed_buy == 0:
            #     idsNums_allowed_buy = 0

            idsNums_allowed_buy = 1
            bk_name = per_bk.name
            hasBuy_lst = []
            hasBuy_nums = 0

            if self.bk_ids.count_documents({'bk_code': per_bk.code, 'date': self.today}) == 0:
                self.bk_ids.insert_one({'date': self.today, 'bk_code': per_bk.code, 'bk_name': bk_name, 'ids_nums': bk_ids_nums, 'idsNums_allowed_buy': idsNums_allowed_buy, 'idsNums_allowed_first': idsNums_allowed_buy, 'hasBuy_nums': hasBuy_nums, 'hasBuy_lst': hasBuy_lst, 'bk_ids_lst': bk_ids_lst})
            else:
                bk_ids_res = self.bk_ids.find_one({'date': self.today, 'bk_code': per_bk.code})
                for r in hasBuy_res:
                    if (r['code'] in bk_ids_lst) and (r['code'] not in bk_ids_res['hasBuy_lst']):
                        hasBuy_lst = bk_ids_res['hasBuy_lst'] + [r['code']]
                        hasBuy_nums = bk_ids_res['hasBuy_nums'] + 1
                        self.bk_ids.update_one({'date': self.today, 'bk_code': per_bk.code}, {'$set': {'hasBuy_nums': hasBuy_nums, 'hasBuy_lst': hasBuy_lst}})

            self.bk_ids_dct[per_bk.code] = {'bk_name': bk_name, 'ids_nums': bk_ids_nums, 'idsNums_allowed_buy': idsNums_allowed_buy, 'idsNums_allowed_first': idsNums_allowed_buy, 'hasBuy_nums': hasBuy_nums, 'hasBuy_lst': hasBuy_lst, 'bk_ids_lst': bk_ids_lst}

        print('处理 bk_ids 数据结束')

        print('*' * 88)
        print('all_jk_list:', self.all_jk_list)
        print('all_jk_buy_list:', self.all_jk_buy_list)
        print('all_jk_sale_list:', self.all_jk_sale_list)
        print(f'总all_jk_list:{len(self.all_jk_list)}')
        print(f'总all_jk_buy_list:{len(self.all_jk_buy_list)}')
        print(f'总all_jk_sale_list:{len(self.all_jk_sale_list)}')
        print('*' * 88)

        # 捕捉最低和最高价格临时记录字典变量
        self.catch_lowest = {}
        self.catch_highest = {}
        self.code_ma = {}
        # 临时存储极值
        self.lowest_price = {}
        self.highest_price = {}
        # 记录清空标记
        self.clean_flag = {}

        # 获取当日交易的股票代码
        for j in self.all_jk_list:
            # 获取所有code的均线ma数据, 有的标的可能当日没有交易数据，比如停牌等，那么需要删除all_jk_list
            try:
                self.code_ma[j] = self.get_ma(j)  # print(j, self.code_ma[j]['beili'], self.code_ma[j]['trd_days'], self.code_ma[j]['trend3'])
            except Exception as e:
                self.logger.error(e)
                self.all_jk_list.remove(j)
                self.all_jk_sale_list.remove(j)
                self.all_jk_buy_list.remove(j)
            else:
                self.catch_lowest[j] = pd.Series(dtype='float64')
                self.catch_highest[j] = pd.Series(dtype='float64')

                # 临时存储极值
                # 临时记录当日最低价格和最高价格
                self.lowest_price[j] = 1000
                self.highest_price[j] = 0

                # 记录清空标记
                self.clean_flag[j] = False
        # exit()
        # 初始话ser和lastreq
        self.ser = pd.Series(dtype='float64')
        self.lastreq = {}

        self.isAppear_top = {}

        # 初始化查询当日bt买入情况, 在jk_buy中，新增一笔买入，那么更新self.bt1_nums 和 self.bt1_codes_lst
        self.bt1_nums = self.hasBuy.count_documents({'bt': 1, 'buy_date': self.today})
        self.bt2_nums = self.hasBuy.count_documents({'bt': 2, 'buy_date': self.today})
        self.bt3_nums = self.hasBuy.count_documents({'bt': 3, 'buy_date': self.today})
        # self.bt4_nums = self.hasBuy.count_documents({'bt': 4, 'buy_date': self.today})
        # self.bt5_nums = self.hasBuy.count_documents({'bt': 5, 'buy_date': self.today})
        self.bt1_lst = self.hasBuy.find({'bt': 1, 'buy_date': self.today})
        self.bt2_lst = self.hasBuy.find({'bt': 2, 'buy_date': self.today})
        self.bt3_lst = self.hasBuy.find({'bt': 3, 'buy_date': self.today})
        # self.bt4_lst = self.hasBuy.find({'bt': 4, 'buy_date': self.today})
        self.bt1_codes_lst = []
        self.bt2_codes_lst = []
        self.bt3_codes_lst = []
        self.bt4_codes_lst = []
        for b in self.bt1_lst:
            self.bt1_codes_lst.append(b['code'])
        for b in self.bt2_lst:
            self.bt2_codes_lst.append(b['code'])
        for c in self.bt3_lst:
            self.bt3_codes_lst.append(c['code'])

        # 设置最大仓位
        self.total_yingkui_money = 0
        yingkui_res = self.trend_rec.find()
        if yingkui_res:
            for r in yingkui_res:
                self.total_yingkui_money += r['yingkui']

        self.logger.info(f"total_yingkui_money:{self.total_yingkui_money}")
        self.allow_all_cangwei = 620000
        self.allow_all_topTradeMoney = 120000 + int(self.total_yingkui_money / 1000) * 1000

        self.dapan = 'decs'
        self.dapan_yes1_macd = self.sh_yestoday.yes1_macd.values[0]
        self.dapan_macd_is_get_smaller = self.sh_yestoday.yes1_macd.values[0] < self.sh_yestoday.yes2_macd.values[0]
        self.dapan_yes1_beili = self.sh_yestoday.beili.values[0]
        self.dapan_xiadie_trend = self.dapan_macd_is_get_smaller and self.sh_yestoday.yes1_highest.values[0] < self.sh_yestoday.yes2_highest.values[0] and self.sh_yestoday.yes1_lowest.values[0] < self.sh_yestoday.yes2_lowest.values[0]
        try:
            if (self.sh_yestoday.trend3.values[0] == 'decs' or self.sh_yestoday.trend4.values[0] == 'decs' or self.sh_yestoday.trend5.values[0] == 'decs' or self.sh_yestoday.trend6.values[0] == 'decs' or self.sz_yestoday.trend3.values[0] == 'decs' or self.sz_yestoday.trend4.values[0] == 'decs' or self.sz_yestoday.trend5.values[0] == 'decs' or self.sz_yestoday.trend6.values[0] == 'decs'):
                self.dapan = 'decs'
            elif (self.sh_yestoday.trend3.values[0] == 'incs' or self.sh_yestoday.trend4.values[0] == 'incs' or self.sh_yestoday.trend5.values[0] == 'incs' or self.sh_yestoday.trend6.values[0] == 'incs' or self.sz_yestoday.trend3.values[0] == 'incs' or self.sz_yestoday.trend4.values[0] == 'incs' or self.sz_yestoday.trend5.values[0] == 'incs' or self.sz_yestoday.trend6.values[0] == 'incs'):
                self.dapan = 'incs'
            else:
                self.dapan = 'noTrend'

        except Exception as e:
            self.logger.error(e)

        # if self.trend_nums <= 10:
        #     fenmu = 100
        # elif self.trend_nums <= 25:
        #     fenmu = 80
        # elif self.trend_nums <= 50:
        #     fenmu = 60
        # elif self.trend_nums <= 75:
        #     fenmu = 50
        # else:
        #     fenmu = 40

        self.per_top_money = self.allow_all_topTradeMoney / 30

        self.trade_lock = thr.Lock()
        self.trade_lock = thr.Lock()
        self.dp_bk_lock = thr.Lock()

        self.pct_sh = 0
        self.pct_sz = 0

        # 设定所有股票总仓位, 在后面交易过程中self.all_cangwei，买入加，卖出减
        try:
            # 若果仓位超标，退出交易
            self.all_nosale_trend_rec = self.trend_rec.find({'isSold': 0})
            self.today_all_hasBuy = self.hasBuy.find({'buy_date': self.today})
            self.all_cangwei = 0
            for a in self.all_nosale_trend_rec:
                self.all_cangwei += a['left_num'] * a['cost']
            for t in self.today_all_hasBuy:
                self.all_cangwei += t['money']

        except Exception as e:
            self.logger.error(e)


        # 实列化code对象
        self.ids_obj_dct = {}
        self.curr_time = None
        for code in self.all_jk_list:
            try:
                ids_selfBk_lst = self.bk_ids_1v1_df[self.bk_ids_1v1_df['ts_code'] == code]['bk_code'].values
                self.ids_obj_dct[code] = trend_one(code, self, ids_selfBk_lst)
                self.logger.info(f'实列化{code},成功, selfBk_lst:{str(ids_selfBk_lst)}')
            except Exception as e:
                self.logger.error(f'实列化{code},失败:{e}')

    def get_trend_logger(self):
        self.logger = logging.getLogger("logger")
        # 判断是否有处理器，避免重复执行
        if not self.logger.handlers:
            # 日志输出的默认级别为warning及以上级别，设置输出info级别
            self.logger.setLevel(logging.DEBUG)
            # 创建一个处理器handler  StreamHandler()控制台实现日志输出
            sh = logging.StreamHandler()
            # 创建一个格式器formatter  （日志内容：当前时间，文件，日志级别，日志描述信息）
            formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(lineno)d line]: %(message)s')

            # 创建一个文件处理器，文件写入日志
            fh = logging.FileHandler(filename=f'{self.logger_path}/trend_all.log', encoding="utf8")
            # 创建一个文件格式器f_formatter
            f_formatter = logging.Formatter(fmt="[%(asctime)s] [%(levelname)s] [%(lineno)d line]: %(message)s", datefmt="%Y/%m/%d %H:%M:%S")

            # 关联控制台日志器—处理器—格式器
            self.logger.addHandler(sh)
            sh.setFormatter(formatter)
            # 设置处理器输出级别
            sh.setLevel(logging.DEBUG)

            # 关联文件日志器-处理器-格式器
            self.logger.addHandler(fh)
            fh.setFormatter(f_formatter)
            # 设置处理器输出级别
            fh.setLevel(logging.DEBUG)

        return self.logger

    # 保持登录
    def keep_login(self):
        time.sleep(30)
        while True:
            time.sleep(1)
            curr_time = datetime.datetime.now()

            # 9点10分，12点40退出重新登录
            # if curr_time.hour == 9 and curr_time.minute == 15 and curr_time.second == 10:
            #     try:
            #         is_login = self.trader.wait.until(EC.text_to_be_present_in_element((By.XPATH, '//*[@id="main"]/div/div[1]/p/span[2]/a'), '退出'))
            #
            #         if is_login:
            #             self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[1]/p/span[2]/a').click()
            #             # send_notice('logout_auto:', f"{curr_time}")
            #             time.sleep(10)
            #     except Exception as e:
            #         self.logger.error(f"logout_fail：{curr_time}, {e}")
            # elif curr_time.hour == 12 and curr_time.minute == 40 and curr_time.second == 10:
            #     try:
            #         is_login = self.trader.wait.until(EC.text_to_be_present_in_element((By.XPATH, '//*[@id="main"]/div/div[1]/p/span[2]/a'), '退出'))
            #
            #         if is_login:
            #             self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[1]/p/span[2]/a').click()
            #             # send_notice('logout_auto:', f"{curr_time}")
            #             time.sleep(10)
            #     except Exception as e:
            #         self.logger.error(f"logout_fail：{curr_time}, {e}")

            # 每隔20秒检查是否登录
            try:
                if curr_time.minute % 59 == 0 and curr_time.second % 59 == 0:
                    res = self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[1]/a').click()
                    time.sleep(10)
                    # self.logger.info(f"login_state:alive, {curr_time}, {res}")  # send_notice('login_state', f"alive, {curr_time}, {res}")
                    print(f"login_state:alive, {curr_time}, {res}")
                elif curr_time.minute % 59 == 0 and curr_time.second % 58 == 0:
                    res = self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[2]/a').click()
                    time.sleep(10)
                    # self.logger.info(f"login_state:alive, {curr_time}, {res}")  # send_notice('login_state', f"alive, {curr_time}, {res}")
                    print(f"login_state:alive, {curr_time}, {res}")
                elif curr_time.minute % 59 == 0 and curr_time.second % 57 == 0:
                    res = self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[3]/a').click()
                    time.sleep(10)
                    # self.logger.info(f"login_state:alive, {curr_time}, {res}")  # send_notice('login_state', f"alive, {curr_time}, {res}")
                    print(f"login_state:alive, {curr_time}, {res}")
                elif curr_time.minute % 59 == 0 and curr_time.second % 56 == 0:
                    res = self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[4]/a').click()
                    time.sleep(10)
                    # self.logger.info(f"login_state:alive, {curr_time}, {res}")  # send_notice('login_state', f"alive, {curr_time}, {res}")
                    print(f"login_state:alive, {curr_time}, {res}")
            except Exception as e:
                self.logger.error(f"登录失效：{curr_time}, {e}")
                # send_notice('login_state', f"lost, {curr_time}, {e}")
                self.trader.login()
                try:
                    res = self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[1]/a').click()  # send_notice('login_state', f"alive_agin, {curr_time}, {res}")
                except Exception as e:
                    self.logger.error(f"重新登录失败：{curr_time}, {e}")
                    # send_notice('login_state', f"lost_agin, {curr_time}, {e}")
                    self.trader.login()

            if curr_time.hour == 12 and curr_time.minute == 50 and curr_time.second >= 57:
                # self.trader.login()
                self.trader = Auto_trade(True)
                time.sleep(5)

            if curr_time.hour >= 15 and curr_time.minute > 3:
                break

    # 修改代码类型
    @classmethod
    def chg_code_type(self, code):
        if len(code) > 7 and code[7:9] == 'SZ':
            code = 'sz' + code[0:6]
        elif len(code) > 7 and code[7:9] == 'SH':
            code = 'sh' + code[0:6]
        elif code[0] == '0' or code[0] == '3':
            code = 'sz' + code[0:6]
        elif code[0] == '6':
            code = 'sh' + code[0:6]
        return code

    def get_basic_data(self, id=0, all_jk_list=None):
        """
        不论数据来源: backtest, trading_debug, trading， 统一返回数据接口，url请求路径增加，规避网络风险，或者后期url被禁风险
        :param ids_lst:
        :return: curr_dp_info, curr_bk_info, lastreq （三个返回值必须在同一curr_time时间节点，保证数据时一致效性）
        """

        curr_dp_info, curr_bk_info, lastreq, curr_time = None, None, None, None

        dp_info = ''
        bk_info = ''
        gn_info = ''
        ids_info = None

        if self.run_type == 'backtest':
            pass
        elif self.run_type == 'trading_debug':

            # 获取数据来源
            per_info = self.trend_basic_data.find_one({'id': id})

            curr_time = per_info['curr_time'][0:19]
            curr_time = datetime.datetime.strptime(curr_time, '%Y-%m-%d %H:%M:%S')

            if per_info:
                try:
                    dp_info = per_info['dp']
                    bk_info = per_info['bk']
                    gn_info = per_info['gn']
                    ids_info = per_info['ids']

                except Exception as e:
                    self.logger.error(e)

        elif self.run_type == 'trading':

            # 每秒获取逐笔交易数据
            curr_time = datetime.datetime.now()
            time_cond = (curr_time.hour >= 9 and curr_time.hour < 15) and ((curr_time.hour == 9 and curr_time.minute >= 27) or curr_time.hour >= 10)

            if time_cond:
            # if 1:

                if self.dp_url_res != '':
                    dp_info = self.dp_url_res.text

                if self.bk_url_res != '':
                    bk_info = self.bk_url_res.text

                if self.gn_url_res != '':
                    gn_info = self.gn_url_res.text

                # if self.ids_url_res != None:
                    # ids_info = copy.deepcopy(self.ids_url_res)
                self.get_ids_fromUrl()
                ids_info = self.ids_url_res.text

        t = time.time()

        if bk_info != '' and gn_info != '':
            curr_bk_info = copy.deepcopy(self.get_bk_data(curr_time, bk_info, gn_info))
        else:
            self.logger.warning(f'bk_info or gn_info is empty {bk_info}, {gn_info}')

        print(f'bk_time:{time.time()-t}', end=', ')
        t = time.time()

        if dp_info != '':
            curr_dp_info = copy.deepcopy(self.get_dp_data(curr_time, dp_info))
        else:
            self.logger.warning(f'dp_info is empty {dp_info}')

        print(f'dp_time:{time.time() - t}', end=', ')
        t = time.time()

        # if ids_info != None and len(ids_info) != 0:
        if ids_info != None:
            self.get_lastreq(ids_info)
            lastreq = copy.deepcopy(self.lastreq)
        else:
            self.logger.warning(f'ids_info is empty {ids_info}')

        print(f'ids_time:{time.time() - t}')

        return curr_dp_info, curr_bk_info, lastreq, curr_time

    def get_lastreq(self, ids_info):

        asyncio.set_event_loop(self.jk_run_loop)
        get_lastreq_loop = asyncio.get_event_loop()

        ids_info = re.split(";", ids_info)
        # 移除尾巴上的/n

        ids_info.pop()
        self.lastreq = {}

        tasks = []
        per_task_num = 20
        slice_num = int(len(ids_info) / per_task_num) + 1
        for s in range(0, slice_num):
            tasks.append(asyncio.ensure_future(self.build_lastreq(ids_info[s * per_task_num:(s + 1) * per_task_num])))
        if len(tasks) != 0:
            get_lastreq_loop.run_until_complete(asyncio.wait(tasks))

    async def build_lastreq(self, res_lst):
        for r in res_lst:
            info = re.split('~', r)
            try:
                code = self.chg_code_type(info[2])
                self.lastreq[code] = {}
                self.lastreq[code]['name'] = info[1]
                self.lastreq[code]['curr_price'] = float(info[3])
                self.lastreq[code]['today_highest'] = float(info[33])
                self.lastreq[code]['today_lowest'] = float(info[34])
                self.lastreq[code]['pct_chg'] = float(info[32])
                self.lastreq[code]['yestoday_close'] = float(info[4])
                self.lastreq[code]['today_open'] = float(info[5])
            except Exception as e:
                print(e, info)

    def jk_run(self):

        asyncio.set_event_loop(self.jk_run_loop)
        loop_jk_run = asyncio.get_event_loop()

        all_data = self.trend_basic_data.count_documents({})

        id = 0
        while True:
            t = time.time()
            id += 1
            if id >= all_data and self.run_type == 'trading_debug':
                break
            self.curr_dp_info, self.curr_bk_info, lastreq, curr_time = self.get_basic_data(id, self.all_jk_list)
            # if 'BK0917' in self.curr_bk_info.keys():
            #     self.logger.debug(str(self.curr_bk_info['BK0917']))
            self.curr_time = curr_time

            if (not self.curr_bk_info) or (not self.curr_dp_info) or (not lastreq):
                print('?', end=',')
                time.sleep(2)
                continue

            # if curr_dp_info['sh']['mk_kline_macd'] != 'decs' and curr_dp_info['sz']['mk_kline_macd'] != 'decs' and curr_dp_info['sh']['curr_macd'] > 0 and curr_dp_info['sz']['curr_macd'] > 0:
            #     self.trade_buy_type = 'am'
            # else:
            #     self.trade_buy_type = 'pm'
            #
            # if curr_dp_info['sh']['mk_kline_macd'] or 'decs' and curr_dp_info['sz']['mk_kline_macd'] == 'decs' or curr_dp_info['sh']['curr_macd'] < 0 or curr_dp_info['sz']['curr_macd'] < 0:
            #     self.trade_sale_type = 'pm'
            # else:
            #     self.trade_sale_type = 'am'

            print('get bk dp time:', time.time() - t)
            tasks_sale = []
            for code in self.all_jk_sale_list:
                try:
                    if self.trade_sale_type == 'am':
                        tasks_sale.append(asyncio.ensure_future(self.ids_obj_dct[code].jk_sale_am(lastreq[code], curr_time, self.curr_bk_info, self.curr_dp_info)))
                    elif self.trade_sale_type == 'pm':
                        tasks_sale.append(asyncio.ensure_future(self.ids_obj_dct[code].jk_sale_pm(lastreq[code], curr_time, self.curr_bk_info, self.curr_dp_info)))

                except Exception as e:
                    self.logger.error(f"{code}创建jk_sale任务失败，{e}")

            if len(tasks_sale) != 0:
                loop_jk_run.run_until_complete(asyncio.wait(tasks_sale))

            tasks_buy = []
            for code in self.all_jk_buy_list:
                # if code not in ['sh600161', 'sh600196', 'sh600208', 'sh600351', 'sh600380']:
                #     continue
                # self.bk_code_lst = self.bk_code[self.bk_code['code'] == code]['bk_code'].values
                # per_curr_bk_info = {}
                # for bk_code in self.bk_code_lst:
                #     per_curr_bk_info[bk_code] = curr_bk_info[bk_code]

                try:
                    if self.trade_buy_type == 'am':
                        tasks_buy.append(asyncio.ensure_future(self.ids_obj_dct[code].jk_buy_am(lastreq[code], curr_time, self.curr_bk_info, self.curr_dp_info)))
                    elif self.trade_buy_type == 'pm':
                        tasks_buy.append(asyncio.ensure_future(self.ids_obj_dct[code].jk_buy_pm(lastreq[code], curr_time, self.curr_bk_info, self.curr_dp_info)))
                except Exception as e:
                    # raise e
                    self.logger.error(f"{code}创建jk_buy任务失败，{e}")

            if len(tasks_buy) != 0:
                loop_jk_run.run_until_complete(asyncio.wait(tasks_buy))

            if curr_time.hour <= 5 or (curr_time.hour >= 15 and curr_time.minute >= 2):
                print('Info: not in trading time, break loop', curr_time)
                break

            print('jk time', time.time() - t)

        print('trade done')
        print('update_trend_rec start')
        # 更新当日trend_rec情况
        self.update_trend_rec()
        print('update_trend_rec done')

        # 更新当日 整体盈利 情况
        self.update_trend_noSale_money()

        # 执行自检
        if self.run_type == 'trading':
            try:
                self.selfcheck.get_reality_cangwei()
                self.selfcheck.get_mongo_cangwei()
                self.selfcheck.update_mongo()
            except Exception as e:
                self.logger.error(e)

    # 获取板块数据情况
    def get_bk_data(self, curr_time, bk_info, gn_info):

        id = 0
        # 创建事件循环对象
        asyncio.set_event_loop(self.jk_run_loop)
        loop_get_bk_data = asyncio.get_event_loop()

        try:
            self.temp_curr_bk_info = {}
            # 获取板块的时时数据信息
            res = bk_info
            res = bk_info[bk_info.index('[') + 1:bk_info.index(']')]
            res = res.split('},')

            # 针对trading_debug模式分时段获取长度不同的板块数据
            if self.run_type == 'trading_debug' and curr_time.second % 15 == 0:
                res = res[0:200]
            else:
                res = res[0:25]

            # if self.run_type == 'trading_debug':
            #     res = res[0:200]

            per_task_num = 15
            slice_num = int(len(res) / per_task_num) + 1
            tasks = []
            for s in range(0, slice_num):
                tasks.append(asyncio.ensure_future(self.build_bk_data(curr_time, res[s * per_task_num:(s + 1) * per_task_num])))
            if len(tasks) != 0:
                loop_get_bk_data.run_until_complete(asyncio.wait(tasks))

            res = gn_info
            res = gn_info[gn_info.index('[') + 1:gn_info.index(']')]

            res = res.split('},')
            # 针对trading_debug模式分时段获取长度不同的板块数据
            if self.run_type == 'trading_debug' and curr_time.second % 15 == 0:
                res = res[0:500]
            else:
                res = res[0:50]
            
            # if self.run_type == 'trading_debug':
            #     res = res[0:500]

            per_task_num = 25
            slice_num = int(len(res) / per_task_num) + 1
            tasks = []
            for s in range(0, slice_num):
                tasks.append(asyncio.ensure_future(self.build_gn_data(curr_time, res[s * per_task_num:(s + 1) * per_task_num])))
            if len(tasks) != 0:
                loop_get_bk_data.run_until_complete(asyncio.wait(tasks))

            return self.temp_curr_bk_info

        except Exception as e:
            raise e
            self.logger.error(e)

        id += 1

    async def build_bk_data(self, curr_time, res_lst):
        for index, r in enumerate(res_lst):
            try:
                ndct = {}
                if r[-1] != '}':
                    r = r + '}'
                dct = json.loads(r)

                if curr_time.hour == 9 and curr_time.minute < 27 and '-' in [dct['f3'], dct['f2'], dct['f18'], dct['f17'], dct['f15'], dct['f16']]:
                    continue

                # if dct['f12'] not in self.bk_jk_lst:
                #     continue

                ndct['bk_name'] = dct['f14']
                ndct['rank'] = index + 1
                ndct['bk_code'] = dct['f12']
                ndct['chg_pct'] = dct['f3']
                ndct['turnover_rate'] = dct['f8']
                ndct['red_code'] = dct['f104']
                ndct['green_code'] = dct['f105']
                # print(ndct['green_code'])
                ndct['green_red'] = ndct['green_code'] / (ndct['green_code'] + ndct['red_code'])
                # ndct['curr_price'] = float(dct['f2']) + random.uniform(1,100)
                ndct['curr_price'] = dct['f2']
                ndct['yes_close'] = dct['f18']
                ndct['open'] = dct['f17']
                ndct['high'] = dct['f15']
                ndct['low'] = dct['f16']
                ndct['type'] = 'bk'
                ndct['num'] = id
                ndct['time'] = str(curr_time)[0:19]

                # 计算板块kline macd 以及macd trend
                bk_close_lst = self.bk_close_lst[ndct['bk_code']]
                # 计算现价macd
                bk_data_list = np.append(bk_close_lst, ndct['curr_price'])
                # print('bk_data_list', bk_data_list)
                curr_kDif, curr_kDea, curr_kDw = talib.MACD(bk_data_list, 3, 8, 5)
                ndct['curr_kDif'], ndct['curr_kDea'], ndct['curr_kDw'] = curr_kDif[-1], curr_kDea[-1], curr_kDw[-1]
                curr_kMacd = (curr_kDif - curr_kDea) * 2
                ndct['curr_kMacd'] = curr_kMacd[-1]

                curr_kMacd_mk = mk(curr_kMacd[-3:])
                ndct['curr_kMacd_trend'] = curr_kMacd_mk[0]
                ndct['curr_kMacd_trend_value'] = curr_kMacd_mk[1]

                # 计算3日价格趋势（最低+收盘+最高）
                curr_bk_detail_df = self.bk_jk_detail[self.bk_jk_detail['ts_code'] == ndct['bk_code']]
                yes1_close = curr_bk_detail_df['yes1_close'].values[0]
                yes1_low = curr_bk_detail_df['yes1_lowest'].values[0]
                yes1_high = curr_bk_detail_df['yes1_highest'].values[0]

                yes2_close = curr_bk_detail_df['yes2_close'].values[0]
                yes2_low = curr_bk_detail_df['yes2_lowest'].values[0]
                yes2_high = curr_bk_detail_df['yes2_highest'].values[0]

                trd3_close = mk([yes2_close, yes1_close, ndct['curr_price']], 0.5)[1]
                trd3_low = mk([yes2_low, yes1_low, ndct['curr_price']], 0.5)[1]
                trd3_high = mk([yes2_high, yes1_high, ndct['curr_price']], 0.5)[1]

                if trd3_close + trd3_high + trd3_low >= 2:
                    ndct['trend3'] = 'incs'
                elif trd3_close + trd3_high + trd3_low <= -2:
                    ndct['trend3'] = 'decs'
                else:
                    ndct['trend3'] = 'noTrend'

                # 计算时时beili情况


                # 计算最低价时macd
                # bk_data_list_low = np.append(bk_close_lst, ndct['low'])
                # low_kline_dif, low_kline_dea, low_kline_dw = talib.MACD(bk_data_list_low, 3, 8, 5)
                # ndct['low_kline_dif'], ndct['low_kline_dea'], ndct['low_kline_dw'] = low_kline_dif[-1], low_kline_dea[-1], low_kline_dw[-1]
                # low_kline_macd = (low_kline_dif - low_kline_dea) * 2
                # ndct['low_kline_macd'] = low_kline_macd[-1]
                # ndct['low_kline_macd_trend'] = mk(low_kline_macd[-3:])[0]

                # 计算板块分时macd
                self.bk_min_price_dct[ndct['bk_code']].append(ndct['curr_price'])
                min_dif, min_dea, min_dw = talib.MACD(np.array(self.bk_min_price_dct[ndct['bk_code']]), 28, 68, 18)
                ndct['min_dif'], ndct['min_dea'], ndct['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                min_macd = (min_dif - min_dea) * 2
                ndct['min_macd'] = min_macd[-1]

                # 在开盘还未计算出macd时，使用price_trend
                ndct['price_trend'] = ''
                # if curr_time.hour == 9 and curr_time.minute <= 45:
                # if np.isnan(ndct['min_macd']) and len(self.bk_min_price_dct[ndct['bk_code']]) > 28:
                #     ndct['price_trend'] = mk(self.bk_min_price_dct[ndct['bk_code']])[0]

                if len(self.bk_min_price_dct[ndct['bk_code']]) >= 28:
                    ndct['price_trend'] = mk(self.bk_min_price_dct[ndct['bk_code']][-28:])[0]

                if len(min_macd[-28:]) <= 1:
                    ndct['min_macd_trend'] = None
                else:
                    ndct['min_macd_trend'] = mk(min_macd[-28:])[0]

                if len(self.bk_min_price_dct[ndct['bk_code']]) > 388:
                    self.bk_min_price_dct[ndct['bk_code']].pop(0)

                # 计算板块的涨跌幅变化趋势
                self.bk_chg_pct_dct[ndct['bk_code']].append(ndct['chg_pct'])
                if len(self.bk_chg_pct_dct[ndct['bk_code']]) <= 28:
                    ndct['mk_chg_pct'] = -10
                else:
                    ndct['mk_chg_pct'] = mk(self.bk_chg_pct_dct[ndct['bk_code']])[1]

                if len(self.bk_chg_pct_dct[ndct['bk_code']]) > 88:
                    self.bk_chg_pct_dct[ndct['bk_code']].pop(0)

                # 计算板块整体红盘的变化趋势
                self.bk_chg_green_dct[ndct['bk_code']].append(ndct['green_red'])
                if len(self.bk_chg_green_dct[ndct['bk_code']]) <= 28:
                    ndct['mk_green_red'] = 'incs'
                    ndct['mk_green_red_value'] = -10
                else:
                    ndct['mk_green_red'] = mk(self.bk_chg_green_dct[ndct['bk_code']])[0]
                    ndct['mk_green_red_value'] = mk(self.bk_chg_green_dct[ndct['bk_code']])[1]

                if len(self.bk_chg_green_dct[ndct['bk_code']]) > 88:
                    self.bk_chg_green_dct[ndct['bk_code']].pop(0)

                ndct['trd_state'] = False
                if ndct['curr_kMacd'] > 0 and (ndct['curr_kMacd_trend'] == 'incs' or ndct['trend3'] == 'incs'):
                    ndct['trd_state'] = True
                else:
                    continue


                self.temp_curr_bk_info[ndct['bk_code']] = ndct

            except Exception as e:
                # raise e
                self.logger.error(e)

    async def build_gn_data(self, curr_time, res_lst):
        for index, r in enumerate(res_lst):
            try:
                ndct = {}
                if r[-1] != '}':
                    r = r + '}'
                dct = json.loads(r)

                if curr_time.hour == 9 and curr_time.minute < 27 and '-' in [dct['f3'], dct['f2'], dct['f18'], dct['f17'], dct['f15'], dct['f16']]:
                    continue

                # if dct['f12'] not in self.bk_jk_lst:
                #     continue

                # 如果有新增概念，暂时这样处理
                if dct['f12'] not in self.bk_kline.keys():
                    continue

                ndct['bk_name'] = dct['f14']
                ndct['rank'] = index + 1
                ndct['bk_code'] = dct['f12']
                ndct['chg_pct'] = dct['f3']
                ndct['turnover_rate'] = dct['f8']
                ndct['red_code'] = int(dct['f104'])
                ndct['green_code'] = int(dct['f105'])
                ndct['green_red'] = ndct['green_code'] / (ndct['green_code'] + ndct['red_code'])
                # ndct['curr_price'] = float(dct['f2']) + random.uniform(1,100)
                ndct['curr_price'] = dct['f2']
                ndct['yes_close'] = dct['f18']
                ndct['open'] = dct['f17']
                ndct['high'] = dct['f15']
                ndct['low'] = dct['f16']
                ndct['type'] = 'gainian'
                ndct['num'] = id
                ndct['time'] = str(curr_time)[0:19]

                # 计算板块kline macd 以及macd trend
                bk_close_lst = self.bk_close_lst[ndct['bk_code']]
                bk_data_list = np.append(bk_close_lst, ndct['curr_price'])
                curr_kDif, curr_kDea, curr_kDw = talib.MACD(bk_data_list, 3, 8, 5)
                ndct['curr_kDif'], ndct['curr_kDea'], ndct['curr_kDw'] = curr_kDif[-1], curr_kDea[-1], curr_kDw[-1]
                curr_kMacd = (curr_kDif - curr_kDea) * 2
                ndct['curr_kMacd'] = curr_kMacd[-1]

                curr_kMacd_mk = mk(curr_kMacd[-3:])
                ndct['curr_kMacd_trend'] = curr_kMacd_mk[0]
                ndct['curr_kMacd_trend_value'] = curr_kMacd_mk[1]

                # 计算3日价格趋势（最低+收盘+最高）
                curr_bk_detail_df = self.bk_jk_detail[self.bk_jk_detail['ts_code'] == ndct['bk_code']]
                yes1_close = curr_bk_detail_df['yes1_close'].values[0]
                yes1_low = curr_bk_detail_df['yes1_lowest'].values[0]
                yes1_high = curr_bk_detail_df['yes1_highest'].values[0]

                yes2_close = curr_bk_detail_df['yes2_close'].values[0]
                yes2_low = curr_bk_detail_df['yes2_lowest'].values[0]
                yes2_high = curr_bk_detail_df['yes2_highest'].values[0]

                trd3_close = mk([yes2_close, yes1_close, ndct['curr_price']], 0.5)[1]
                trd3_low = mk([yes2_low, yes1_low, ndct['curr_price']], 0.5)[1]
                trd3_high = mk([yes2_high, yes1_high, ndct['curr_price']], 0.5)[1]

                if trd3_close + trd3_high + trd3_low >= 2:
                    ndct['trend3'] = 'incs'
                elif trd3_close + trd3_high + trd3_low <= -2:
                    ndct['trend3'] = 'decs'
                else:
                    ndct['trend3'] = 'noTrend'

                # 计算最低价时macd
                # bk_data_list_low = np.append(bk_close_lst, ndct['low'])
                # low_kline_dif, low_kline_dea, low_kline_dw = talib.MACD(bk_data_list_low, 3, 8, 5)
                # ndct['low_kline_dif'], ndct['low_kline_dea'], ndct['low_kline_dw'] = low_kline_dif[-1], low_kline_dea[-1], low_kline_dw[-1]
                # low_kline_macd = (low_kline_dif - low_kline_dea) * 2
                # ndct['low_kline_macd'] = low_kline_macd[-1]
                # ndct['low_kline_macd_trend'] = mk(low_kline_macd[-3:])[0]

                # 计算板块分时macd
                self.bk_min_price_dct[ndct['bk_code']].append(ndct['curr_price'])
                min_dif, min_dea, min_dw = talib.MACD(np.array(self.bk_min_price_dct[ndct['bk_code']]), 28, 68, 18)
                ndct['min_dif'], ndct['min_dea'], ndct['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                min_macd = (min_dif - min_dea) * 2
                ndct['min_macd'] = min_macd[-1]

                # 在开盘还未计算出macd时，使用price_trend
                ndct['price_trend'] = ''
                # if curr_time.hour == 9 and curr_time.minute <= 45:
                # if np.isnan(ndct['min_macd']) and len(self.bk_min_price_dct[ndct['bk_code']]) > 28:
                #     ndct['price_trend'] = mk(self.bk_min_price_dct[ndct['bk_code']])[0]

                if len(self.bk_min_price_dct[ndct['bk_code']]) >= 28:
                    ndct['price_trend'] = mk(self.bk_min_price_dct[ndct['bk_code']][-28:])[0]

                if len(min_macd[-28:]) <= 1:
                    ndct['min_macd_trend'] = None
                else:
                    ndct['min_macd_trend'] = mk(min_macd[-28:])[0]
                if len(self.bk_min_price_dct[ndct['bk_code']]) > 388:
                    self.bk_min_price_dct[ndct['bk_code']].pop(0)

                # 计算板块的涨跌幅变化趋势
                if ndct['bk_code'] not in self.bk_chg_pct_dct.keys():
                    self.bk_chg_pct_dct[ndct['bk_code']] = []
                self.bk_chg_pct_dct[ndct['bk_code']].append(ndct['chg_pct'])

                if len(self.bk_chg_pct_dct[ndct['bk_code']]) > 88:
                    self.bk_chg_pct_dct[ndct['bk_code']].pop(0)

                if len(self.bk_chg_pct_dct[ndct['bk_code']]) <= 28:
                    ndct['mk_chg_pct'] = -10
                else:
                    ndct['mk_chg_pct'] = mk(self.bk_chg_pct_dct[ndct['bk_code']])[1]

                # 计算板块整体红盘的变化趋势
                self.bk_chg_green_dct[ndct['bk_code']].append(ndct['green_red'])
                if len(self.bk_chg_green_dct[ndct['bk_code']]) <= 28:
                    ndct['mk_green_red'] = 'incs'
                    ndct['mk_green_red_value'] = -10
                else:
                    ndct['mk_green_red'] = mk(self.bk_chg_green_dct[ndct['bk_code']])[0]
                    ndct['mk_green_red_value'] = mk(self.bk_chg_green_dct[ndct['bk_code']])[1]

                if len(self.bk_chg_green_dct[ndct['bk_code']]) > 88:
                    self.bk_chg_green_dct[ndct['bk_code']].pop(0)

                ndct['trd_state'] = False
                if ndct['curr_kMacd'] > 0 and (ndct['curr_kMacd_trend'] == 'incs' or ndct['trend3'] == 'incs'):
                    ndct['trd_state'] = True
                else:
                    continue

                self.temp_curr_bk_info[ndct['bk_code']] = ndct

            except Exception as e:
                # raise e
                self.logger.error(e)

    # 获取大盘数据情况
    def get_dp_data(self, curr_time, dp_info):

        res = re.split(";", dp_info)
        # 移除尾巴上的/n
        res.pop()
        temp_curr_dp_info = {'sh': {}, 'sz': {}}
        for r in res:
            info = re.split('~', r)

            if info[2] == '000001':
                dp_code = 'sh'
                # 计算大盘kline macd
                self.sh_price_lst[-1] = float(info[3])
                dif, dea, dw = talib.MACD(self.sh_price_lst, 3, 8, 5)
                macd = (dif - dea) * 2
                self.sh_macd_lst[-1] = macd[-1]
                temp_curr_dp_info[dp_code]['mk_kline_macd'] = mk(self.sh_macd_lst[-3:])[0]

                # 计算大盘分时 macd
                self.dp_min_price_dct['sh'].append(float(info[3]))
                self.dp_min_price_dct['time'].append(curr_time)

                min_dif, min_dea, min_dw = talib.MACD(np.array(self.dp_min_price_dct['sh']), 28, 68, 18)
                temp_curr_dp_info[dp_code]['min_dif'], temp_curr_dp_info[dp_code]['min_dea'], temp_curr_dp_info[dp_code]['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                min_macd = (min_dif - min_dea) * 2
                temp_curr_dp_info[dp_code]['min_macd'] = min_macd[-1]
                if len(min_macd[-28:]) <= 1:
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = None
                else:
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = mk(min_macd[-28:])[0]

                if len(self.dp_min_price_dct['sh']) > 388:
                    self.dp_min_price_dct['sh'].pop(0)
                    self.dp_min_price_dct['time'].pop(0)

                # 计算大盘pct_chg mk
                self.dp_chg_pct_dct['sh'].append(float(info[3]))

                if len(self.dp_chg_pct_dct['sh']) > 38:
                    self.dp_chg_pct_dct['sh'].pop(0)

                if len(self.dp_chg_pct_dct['sh']) < 28:
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = -10
                else:
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = mk(self.dp_chg_pct_dct['sh'])[1]

            elif info[2] == '399001':
                dp_code = 'sz'
                self.sz_price_lst[-1] = float(info[3])

                dif, dea, dw = talib.MACD(self.sz_price_lst, 3, 8, 5)
                macd = (dif - dea) * 2
                self.sz_macd_lst[-1] = macd[-1]
                temp_curr_dp_info[dp_code]['mk_kline_macd'] = mk(self.sz_macd_lst[-3:])[0]

                # 计算大盘分时 macd
                self.dp_min_price_dct['sz'].append(float(info[3]))

                min_dif, min_dea, min_dw = talib.MACD(np.array(self.dp_min_price_dct['sz']), 28, 68, 18)
                temp_curr_dp_info[dp_code]['min_dif'], temp_curr_dp_info[dp_code]['min_dea'], temp_curr_dp_info[dp_code]['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                min_macd = (min_dif - min_dea) * 2
                temp_curr_dp_info[dp_code]['min_macd'] = min_macd[-1]
                if len(min_macd[-28:]) <= 1:
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = None
                else:
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = mk(min_macd[-28:])[0]

                if len(self.dp_min_price_dct['sz']) > 388:
                    self.dp_min_price_dct['sz'].pop(0)

                if len(self.dp_chg_pct_dct['sz']) > 38:
                    self.dp_chg_pct_dct['sz'].pop(0)

                if len(self.dp_chg_pct_dct['sz']) < 28:
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = -10
                else:
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = mk(self.dp_chg_pct_dct['sz'])[1]

            elif info[2] not in ['000001', '399001']:
                continue

            # print(info[2], temp_curr_dp_info)
            temp_curr_dp_info[dp_code]['curr_price'] = float(info[3])
            temp_curr_dp_info[dp_code]['today_highest'] = float(info[33])
            temp_curr_dp_info[dp_code]['today_lowest'] = float(info[34])
            temp_curr_dp_info[dp_code]['pct_chg'] = float(info[32])
            temp_curr_dp_info[dp_code]['yestoday_close'] = float(info[4])
            temp_curr_dp_info[dp_code]['today_open'] = float(info[5])
            temp_curr_dp_info[dp_code]['curr_dif'] = dif[-1]
            temp_curr_dp_info[dp_code]['curr_dea'] = dea[-1]
            temp_curr_dp_info[dp_code]['curr_macd'] = macd[-1]

        return temp_curr_dp_info

    # 获取板块数据情况
    def get_bk_dp_fromUrl(self):

        # 获取行业板块
        # url = "https://54.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112305894667514355395_1667748273086&pn=1&pz=500&po=1&np=1&fields=f12%2Cf13%2Cf14%2Cf62&fid=f62&fs=m%3A90%2Bt%3A2&ut=b2884a393a59ad64002292a3e90d46a5&_=1667748273093"

        # f14, f12, f3, f2, f18, f17, f15, f16, f8, f104, f105

        # f14 name, f12 bk_code, f3 chg_pct, f8 turnover_rate, f104 red_code
        # f105 green_code, f2 curr_price, f18 yes_close, f17 open, f15 high, f16 low

        t = time.time()

        while True:
            # 每秒获取逐笔交易数据
            curr_time = datetime.datetime.now()
            time_cond = (curr_time.hour >= 9 and curr_time.hour < 15) and ((curr_time.hour == 9 and curr_time.minute >= 27) or curr_time.hour >= 10)

            if time_cond:
            # if 1:

                # 获取大盘数据
                url = "https://qt.gtimg.cn/q=sh000001,sz399001&r=926277345"

                try:
                    self.dp_url_res = Proxy_url.urlget(url)
                except Exception as e:
                    self.logger.error(str(e) + ' will try again')
                    self.dp_url_res = req.get(url)


                # 获取板块和概念数据
                if curr_time.second % 29 == 0:
                    bk_url_num = '100'
                    gn_url_num = '500'
                else:
                    bk_url_num = '28'
                    gn_url_num = '88'

                try:
                    # 获取板块的时时数据信息
                    self.per_bk_gn = None
                    bk_url = f"http://54.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112407416912952598056_1671491278146&pn=1&pz={bk_url_num}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:90+t:2+f:!50&fields=f14,f12,f3,f2,f18,f17,f15,f16,f8,f104,f105&_=1671491278158"
                    # print(bk_url)
                    res = Proxy_url.urlget(bk_url)
                    # res = req.get(bk_url)
                    res.encoding = 'utf-8'
                    self.bk_url_res = res

                    gainian_url = f"http://51.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124003105763407314488_1671663240679&pn=1&pz={gn_url_num}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:90+t:3+f:!50&fields=f14,f12,f3,f2,f18,f17,f15,f16,f8,f104,f105&_=1671663240687"
                    # print(gainian_url)
                    res = Proxy_url.urlget(gainian_url)
                    # res = req.get(gainian_url)
                    res.encoding = 'utf-8'
                    self.gn_url_res = res

                except Exception as e:
                    # raise e
                    self.logger.error(e)

    # 获取ids数据情况
    def get_ids_fromUrl(self):

        # url 获取ids数据
        ncode = []
        for c in self.all_jk_list:
            ncode.append(self.chg_code_type(c))
        all_list_2str = ','.join(ncode)

        url = "https://qt.gtimg.cn/q=" + all_list_2str + "&r=926277345"
        try:
            self.ids_url_res = Proxy_url.urlget(url)  # print(res)
        except Exception as e:
            self.logger.error(str(e) + ' will try again')
            self.ids_url_res = req.get(url)
            print('try again:', self.ids_url_res)

        # url 获取ids数据
        # ncode = []
        # t = time.time()
        # for c in self.all_jk_list:
        #     ncode.append(self.chg_code_type(c))
        # all_list_2str = ','.join(ncode)
        # print('join time:', time.time()-t)
        # t = time.time()
        # # url = "https://qt.gtimg.cn/q=" + all_list_2str + "&r=926277345"
        # try:
        #     self.ids_url_res = Proxy_url.urlget(url)  # print(res)
        # except Exception as e:
        #     self.logger.error(str(e) + ' will try again')
        #     self.ids_url_res = req.get(url)
        #     print('try again:', self.ids_url_res)
        # print('url get time:', time.time()-t)


    def dp(self):
        while True:
            # 每秒获取逐笔交易数据
            curr_time = datetime.datetime.now()
            if curr_time.hour >= 9 and curr_time.hour <= 15:
                if (curr_time.hour == 11 and curr_time.minute > 30) or curr_time.hour == 12:
                    continue
                elif curr_time.hour >= 15 and curr_time.minute > 3:
                    break
                elif (curr_time.hour == 9 and curr_time.minute >= 25) or curr_time.hour >= 10:
                    # 获取当日两市上涨概况
                    url = 'http://qt.gtimg.cn/?q=s_sz399001,s_sz399300,s_sh000016,s_sz399004,bkqtRank_A_sh,bkqtRank_B_sh,bkqtRank_A_sz,bkqtRank_B_sz&_=1595790947726'
                    res = req.get(url)
                    res = re.split(';', res.text)
                    sh = re.split('~', res[4])
                    sz = re.split('~', res[6])
                    # 获取沪市上涨的股票
                    sh_url = 'http://stock.gtimg.cn/data/view/rank.php?t=rankash/chr&p=1&o=0&l=' + str(int(sh[2])) + '&v=list_data'
                    sh_res = req.get(sh_url)
                    sh_res = re.split("'", sh_res.text)
                    sh_res = re.split(',', sh_res[3])

                    # 获取深市上涨的股票
                    sz_url = 'http://stock.gtimg.cn/data/view/rank.php?t=rankasz/chr&p=1&o=0&l=' + str(int(sz[2])) + '&v=list_data'
                    sz_res = req.get(sz_url)
                    sz_res = re.split("'", sz_res.text)
                    sz_res = re.split(',', sz_res[3])
                    codelist2 = sh_res + sz_res

                    # 获取红盘占比，得到市场的情绪
                    all_sh = 1951
                    all_sz = 2469

                    self.pct_sh = len(sh_res) / all_sh
                    self.pct_sz = len(sz_res) / all_sz

                    # return pct_sh,pct_sz

            if curr_time.hour >= 15 and curr_time.minute >= 1:
                break

            time.sleep(1)

    def get_ma(self, code):
        # 获取历史交易的收盘价，最高价格，最低价，用于计算均价
        self.code_df = self.all_trend_df[self.all_trend_df['ts_code'] == code]
        ma_dict = {}
        try:
            ma_dict['beili'] = self.code_df['beili'].values[0]
            ma_dict['trd_days'] = self.code_df['trd_days'].values[0]
            ma_dict['sum_ma4'] = self.code_df['sum4'].values[0]
            ma_dict['sum_ma5'] = self.code_df['sum5'].values[0]
            ma_dict['sum_ma6'] = self.code_df['sum6'].values[0]
            ma_dict['sum_ma7'] = self.code_df['sum7'].values[0]
            ma_dict['sum_ma8'] = self.code_df['sum8'].values[0]
            ma_dict['yes2_ma5'] = self.code_df['yes2_ma5'].values[0]
            ma_dict['yes1_ma5'] = self.code_df['yes1_ma5'].values[0]
            ma_dict['yes1_ma4'] = self.code_df['yes1_ma4'].values[0]
            ma_dict['yes2_ma4'] = self.code_df['yes2_ma4'].values[0]
            ma_dict['yes1_ma6'] = self.code_df['yes1_ma6'].values[0]
            ma_dict['yes2_ma6'] = self.code_df['yes2_ma6'].values[0]
            ma_dict['yes1_ma7'] = self.code_df['yes1_ma7'].values[0]
            ma_dict['yes2_ma7'] = self.code_df['yes2_ma7'].values[0]
            ma_dict['yes1_ma8'] = self.code_df['yes1_ma8'].values[0]
            ma_dict['yes2_ma8'] = self.code_df['yes2_ma8'].values[0]
            ma_dict['yes1_close'] = self.code_df['yes1_close'].values[0]
            ma_dict['yes2_close'] = self.code_df['yes2_close'].values[0]
            ma_dict['yes1_lowest'] = self.code_df['yes1_lowest'].values[0]
            ma_dict['yes2_lowest'] = self.code_df['yes2_lowest'].values[0]
            ma_dict['yes1_highest'] = self.code_df['yes1_highest'].values[0]
            ma_dict['yes2_highest'] = self.code_df['yes2_highest'].values[0]

            ma_dict['yes1_macd'] = self.code_df['yes1_macd'].values[0]
            ma_dict['yes2_macd'] = self.code_df['yes2_macd'].values[0]

            ma_dict['trend3'] = self.code_df['trend3'].values[0]
            ma_dict['trend4'] = self.code_df['trend4'].values[0]
            ma_dict['trend5'] = self.code_df['trend5'].values[0]
            ma_dict['trend6'] = self.code_df['trend6'].values[0]

            ma_dict['trend8'] = self.code_df['trend8'].values[0]
            ma_dict['trend9'] = self.code_df['trend9'].values[0]
            ma_dict['trend10'] = self.code_df['trend10'].values[0]

            ma_dict['trend12'] = self.code_df['trend12'].values[0]
            ma_dict['trend13'] = self.code_df['trend13'].values[0]
            ma_dict['trend14'] = self.code_df['trend14'].values[0]

            ma_dict['trend20'] = self.code_df['trend20'].values[0]
            ma_dict['trend50'] = self.code_df['trend50'].values[0]
        except IndexError as e:
            try:
                print('code:', code)
                print('today:', self.today)
                print('today:', self.today)
                print('len(self.all_trend_df)', len(self.all_trend_df))
                print("len(self.all_trend_df[self.all_trend_df['ts_code'] == code])", len(self.all_trend_df[self.all_trend_df['ts_code'] == code]))
                print("self.all_trend_df[self.all_trend_df['ts_code'] == code]", self.all_trend_df[self.all_trend_df['ts_code'] == code])
            except Exception as e:
                self.logger.error(e)

        return ma_dict

    def calculate_profit_one(self, code):

        # 获取昨日清仓盈利情况,昨日可能买了一笔，可能买了多笔
        hasSale = self.trend_rec.find({'code': code, 'isSold': 1})
        noSale = self.trend_rec.find({'code': code, 'isSold': 0})
        hasSale_yingkui = 0
        noSale_yingkui = 0
        sale_money = 0
        noSale_money = 0
        for y in hasSale:
            hasSale_yingkui += y['yingkui']
            sale_money += y['money']
        for k in noSale:
            noSale_yingkui += k['yingkui']
            noSale_money += k['money']

        # 更新到总表里
        total_yingkui = noSale_yingkui + hasSale_yingkui
        # self.allList.update_one({'code': code}, {'$set': {'yingkui': total_yingkui}})
        self.logger.info('截止' + str(self.today) + ' 日 ' + str(code) + ' 盈亏：' + str(total_yingkui) + ' 未卖出金额：' + str(noSale_money) + ' 已卖出金额：' + str(sale_money))

    def calculate_profit(self):

        # 获取昨日清仓盈利情况,昨日可能买了一笔，可能买了多笔
        hasSale = self.trend_rec.find({'isSold': 1})
        noSale = self.trend_rec.find({'isSold': 0})
        hasSale_yingkui = 0
        noSale_yingkui = 0
        sale_money = 0
        noSale_money = 0
        for y in hasSale:
            hasSale_yingkui += y['yingkui']
            sale_money += y['money']
        for k in noSale:
            noSale_yingkui += k['yingkui']
            noSale_money += k['money']

        # 更新到总表里
        total_yingkui = noSale_yingkui + hasSale_yingkui
        # self.allList.update_one({'code': code}, {'$set': {'yingkui': total_yingkui}})
        self.logger.info('截止' + str(self.today) + ' 总盈亏：' + str(total_yingkui) + ' 未卖出金额：' + str(noSale_money) + ' 已卖出金额：' + str(sale_money))

    def get_reality_cangwei(self):
        pass  # while True:  #     curr_time = datetime.datetime.now()  #     if curr_time.minute % 25 == 0 and curr_time.second % 59 == 0:  #         # print("get_reality_cangwei is alive: ", curr_time)  #         pass  #     time.sleep(1)  #     if curr_time.hour >= 9 and curr_time.hour <= 14:  #         if (curr_time.hour == 11 and curr_time.minute > 30) or curr_time.hour == 12:  #             continue  #         elif curr_time.hour >= 15 and curr_time.minute > 1:  #             break  #         elif (curr_time.hour == 9 and curr_time.minute >= 36) or curr_time.hour >= 10:  #  #             if curr_time.minute % 18 == 0 and curr_time.second % 39 == 0:  #                 self.trend_reality.drop()  #                 self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[1]/a').click()  #                 buy_btn = self.trader.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tabBody"]')))  #                 if buy_btn:  #                     res = self.trader.driver.find_element_by_xpath('//*[@id="tabBody"]')  #                     res = res.text.split('\n')  #                     for r in res:  #                         per = r.split(' ')  #                         if len(per) != 12:  #                             continue  #                         # print(per)  #                         # 更新当前实际持仓情况  #                         self.trend_reality.insert_one({'code': self.chg_code_type(per[0]), 'hNum': per[2], 'uNum': per[3], 'money': per[6], 'cost': per[4], 'price': per[5], 'pct': per[8], 'yingkui': per[7]})  #  #                         # 查询系统期望可用持仓情况（可用持仓情况只需要查询trend_rec, 不可用持仓则查看hasbuy）  #                         mongo_res = self.trend_rec.find({'code': self.chg_code_type(per[0]), 'isSold': 0})  #                         mongo_num = 0  #                         if mongo_res:  #                             for m in mongo_res:  #                                 mongo_num += m['left_num']  #  #                             if int(per[3]) > int(mongo_num) and per[0][0] not in ['5', '1']:  #                                 # 卖出不一致仓位  #                                 sold_num = int(per[3]) - int(mongo_num)  #                                 sale_price = round(float(per[5]) * 0.99, 2)  #                                 try:  #                                     sold_num = 'all' if sold_num <= 100 else int(sold_num / 100) * 100  #                                     with self.trade_lock:  #                                         res = self.trader.auto_sale_chrome(self.chg_code_type(per[0]), sale_price, sold_num)  #                                         self.logger.info(f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, curr_price:{float(per[5])}, num:{sold_num}, response:{res}")  # # send_notice('sold_point_get_reality_cangwei_success', f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, curr_price:{float(per[5])}, num:{sold_num}, response:{res}")  # time.sleep(5)  #                                 except Exception as e:  #                                     self.logger.error(f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, time:{curr_time}, sold_type:卖出不一致, e:{e}")  # # send_notice('sold_point_get_reality_cangwei_faild', f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, time:{curr_time}, sold_type:卖出不一致, e:{e}")  # time.sleep(5)

    def update_trend_rec(self):
        # 更新当日trend_rec情况
        if self.run_type == 'backtest':
            today_hasBuy = self.hasBuy.find({'buy_date': self.today})
            for thb in today_hasBuy:
                print(thb)
                k = thb['code']
                num = 0
                buyMoney = 0
                hasBuy = self.hasBuy.find({'code': k, 'buy_date': self.today})
                curr_time = datetime.datetime.now()
                bts = 0

                for h in hasBuy:
                    num += h['num']
                    buyMoney += h['money']
                    bts += 1
                if num != 0:
                    cost = round(buyMoney / num, 4)
                    print({'code': k, 'price': cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'highest_price': cost})
                    count = self.trend_rec.count_documents({'code': k, 'buy_date': self.today})

                    if count == 0 and num != 0:
                        # self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price': self.lastreq[k]['curr_price'], 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0, 'highest_price': cost})  # 获取每支个股最近插入的背离数据
                        self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price': cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0, 'highest_price': cost})  # 获取每支个股最近插入的背离数据

        elif self.run_type in ['trading', 'trading_debug']:
            today_hasBuy = self.hasBuy.find({'buy_date': self.today})
            for thb in today_hasBuy:
                print(thb)
                k = thb['code']
                num = 0
                buyMoney = 0
                hasBuy = self.hasBuy.find({'code': k, 'buy_date': self.today})
                curr_time = datetime.datetime.now()
                bts = 0

                for h in hasBuy:
                    num += h['num']
                    buyMoney += h['money']
                    bts += 1
                if num != 0:
                    cost = round(buyMoney / num, 4)
                    print({'code': k, 'price': cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'highest_price': cost})
                    count = self.trend_rec.count_documents({'code': k, 'buy_date': self.today})

                    if count == 0 and num != 0:
                        # self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price': self.lastreq[k]['curr_price'], 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0,
                        #                            'highest_price': cost})  # 获取每支个股最近插入的背离数据  # self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price':cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0, 'highest_price': cost})  # 获取每支个股最近插入的背离数据
                        # #
                        self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price': cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0, 'highest_price': cost,
                                                   'trade_buy_type': self.trade_buy_type})  # 获取每支个股最近插入的背离数据  # self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price':cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0, 'highest_price': cost})  # 获取每支个股最近插入的背离数据

    def update_trend_noSale_money(self):
        if self.run_type == 'backtest':
            file_name = str(os.path.basename(__file__).split('.')[0])

            res_nosale_money = self.fd[file_name + 'trend_rec'].find({'isSold': 0})
            res_sale_money = self.fd[file_name + 'trend_rec'].find({'isSold': 1})

            today_nosale_money = 0
            today_sale_money = 0
            today_total_yingkui = 0
            for r in res_nosale_money:
                today_nosale_money += r['left_num'] * r['price']
                today_total_yingkui += (r['yingkui1'] + r['yingkui2'] + r['yingkui3'] + r['left_num'] * (r['curr_price'] - r['cost']))
            for r in res_sale_money:
                today_sale_money += r['money']
                today_total_yingkui += r['yingkui']

            yes_info = self.fd[file_name + 'noSale_money'].find_one({'date': self.yestoday})

            if not yes_info:
                highest_yingkui = today_total_yingkui
                huicelv = 0
                highest_huicelv = 0
            elif (yes_info and yes_info['highest_yingkui'] < today_total_yingkui) or not yes_info:
                highest_yingkui = today_total_yingkui
                huicelv = 0
                highest_huicelv = yes_info['highest_huicelv']
            else:
                highest_yingkui = yes_info['highest_yingkui']
                try:
                    huicelv = abs(round((yes_info['highest_yingkui'] - today_total_yingkui) / yes_info['highest_yingkui'], 3))
                except Exception as e:
                    print(e)
                    huicelv = 0
                if yes_info['highest_huicelv'] < huicelv:
                    highest_huicelv = huicelv
                else:
                    highest_huicelv = yes_info['highest_huicelv']

            self.fd[file_name + 'noSale_money'].insert_one({'date': self.today, 'noSale_money': today_nosale_money, 'sale_money': today_sale_money, 'yingkui': today_total_yingkui, 'highest_yingkui': highest_yingkui, 'huicelv': huicelv, 'highest_huicelv': highest_huicelv})

        elif self.run_type == 'trading':
            res_nosale_money = self.fd['trend_rec'].find({'isSold': 0})
            res_sale_money = self.fd['trend_rec'].find({'isSold': 1})

            today_nosale_money = 0
            today_sale_money = 0
            today_total_yingkui = 0
            for r in res_nosale_money:
                today_nosale_money += r['left_num'] * r['price']
                today_total_yingkui += (r['yingkui1'] + r['yingkui2'] + r['yingkui3'] + r['left_num'] * (r['curr_price'] - r['cost']))
            for r in res_sale_money:
                today_sale_money += r['money']
                today_total_yingkui += r['yingkui']

            self.fd['trend_noSale_money'].insert_one({'date': self.today, 'noSale_money': today_nosale_money, 'sale_money': today_sale_money, 'yingkui': today_total_yingkui})

# @jit
def mk(x, alpha=0.5):  # 0<alpha<0.5 1-alpha/2为置信度
    n = len(x)

    # 计算S的值
    s = 0
    for j in range(n - 1):
        for i in range(j + 1, n):
            s += np.sign(x[i] - x[j])

    # 判断x里面是否存在重复的数，输出唯一数队列unique_x,重复数数量队列tp
    unique_x, tp = np.unique(x, return_counts=True)
    g = len(unique_x)

    # 计算方差VAR(S)
    if n == g:  # 如果不存在重复点
        var_s = (n * (n - 1) * (2 * n + 5)) / 18
    else:
        var_s = (n * (n - 1) * (2 * n + 5) - np.sum(tp * (tp - 1) * (2 * tp + 5))) / 18

    # 计算z_value
    if n <= 10:  # n<=10属于特例
        z = s / (n * (n - 1) / 2)
    else:
        if s > 0:
            z = (s - 1) / np.sqrt(var_s)
        elif s < 0:
            z = (s + 1) / np.sqrt(var_s)
        else:
            z = 0

    # 计算p_value，可以选择性先对p_value进行验证
    p = 2 * (1 - norm.cdf(abs(z)))

    # 计算Z(1-alpha/2)
    h = abs(z) > norm.ppf(1 - alpha / 2)

    # 趋势判断
    if (z < 0) and h:
        trend = 'decs'  # trend = -1
    elif (z > 0) and h:
        trend = 'incs'  # trend = 1
    else:
        trend = 'notrend'  # trend = 0

    return (trend, z)

class control_panel(QMainWindow, Ui_MainWindow):
    def __init__(self, trend):
        self.trend = trend
        super(control_panel, self).__init__()
        self.setupUi(self)
        self.timer = QTimer(self)

        self.kline_dct = {}
        self.per_ids_widget_dct = {}
        self.per_bk_label_dct = {}
        k = 0
        try:
            for code in list(self.trend.ids_obj_dct.keys()):
                if code in ['sh600161', 'sh600196', 'sh600208', 'sh600351', 'sh600380']:
                    self.kline_dct[code] = kline(self.trend, code)
                    self.per_ids_widget_dct[code] = per_ids_widget()

                    self.per_ids_widget_dct[code].horizontalLayout.addWidget(self.kline_dct[code])

                    self.verticalLayout.addWidget(self.per_ids_widget_dct[code])

                    self.write_ids_basic_info(code)

                    self.timer.timeout.connect(self.kline_dct[code].draw_kline)
                    k += 1

            self.timer.timeout.connect(self.update_label)

            self.verticalLayoutWidget.setGeometry(QtCore.QRect(10, 10, 2471, 500*k))
            self.timer.start(888)
        except Exception as e:
            print('??????', e)
        # self.create_timer()

    def create_timer(self):
        self.timer = QTimer(self)
        for code in list(self.trend.ids_obj_dct.keys()):
            if code in ['sh600161', 'sh600196', 'sh600208', 'sh600351', 'sh600380']:
                self.timer.timeout.connect(lambda: self.update_label(code))
                self.timer.timeout.connect(self.kline_dct[code].draw_kline)
        self.timer.start(888)

    def update_label(self):
        try:
            # print(self.trend.ids_obj_dct[code].ids_mMacd, self.trend.ids_obj_dct[code].today_lowest, self.trend.ids_obj_dct[code].today_highest, self.trend.ids_obj_dct[code].curr_price)
            for code in list(self.trend.ids_obj_dct.keys()):
                if code in ['sh600161', 'sh600196', 'sh600208', 'sh600351', 'sh600380']:
                    self.per_ids_widget_dct[code].label_ids_mMacd_val.setText(str(round(self.trend.ids_obj_dct[code].ids_mMacd, 5)))
                    self.per_ids_widget_dct[code].label_ids_mDif_val.setText(str(round(self.trend.ids_obj_dct[code].ids_mDif, 5)))
                    self.per_ids_widget_dct[code].label_ids_kMacd_val.setText(str(round(self.trend.ids_obj_dct[code].ids_kMacd, 5)))
                    self.per_ids_widget_dct[code].label_ids_kMacd_trd_val.setText(str(self.trend.ids_obj_dct[code].ids_kMacd_trd))
                    self.per_ids_widget_dct[code].label_ids_kDif_val.setText(str(round(self.trend.ids_obj_dct[code].ids_kDif, 5)))
                    self.per_ids_widget_dct[code].label_ids_lowest_val.setText(str(self.trend.ids_obj_dct[code].today_lowest))
                    self.per_ids_widget_dct[code].label_ids_highest_val.setText(str(self.trend.ids_obj_dct[code].today_highest))
                    self.per_ids_widget_dct[code].label_currTime_val.setText(str(self.trend.curr_time)[-8:])
                    self.per_ids_widget_dct[code].label_ids_trend3_val.setText(str(self.trend.ids_obj_dct[code].ids_curr_trend3))

                    # 更新板块数据
                    j = 1
                    for per_bk in self.trend.bk_code[self.trend.bk_code['code'] == code].itertuples():
                        self.per_bk_label_dct[code][j]['label_bk_code_1'].setText(per_bk.bk_code)
                        self.per_bk_label_dct[code][j]['label_bk_name_1'].setText(per_bk.bk_name)
                        if per_bk.bk_code in self.trend.curr_bk_info.keys():
                            self.per_bk_label_dct[code][j]['label_bk_kMacd_1'].setText(str(round(self.trend.curr_bk_info[per_bk.bk_code]['curr_kMacd'], 3)))
                            self.per_bk_label_dct[code][j]['label_bk_kDif_1'].setText(str(round(self.trend.curr_bk_info[per_bk.bk_code]['curr_kDif'], 3)))
                            self.per_bk_label_dct[code][j]['label_bk_kMacd_trd_1'].setText(str(self.trend.curr_bk_info[per_bk.bk_code]['curr_kMacd_trend']))
                            self.per_bk_label_dct[code][j]['label_bk_trend3_1'].setText(str(self.trend.curr_bk_info[per_bk.bk_code]['trend3']))
                            self.per_bk_label_dct[code][j]['label_bk_beili_1'].setText('None')
                        else:
                            self.per_bk_label_dct[code][j]['label_bk_kMacd_1'].setText('None')
                            self.per_bk_label_dct[code][j]['label_bk_kDif_1'].setText('None')
                            self.per_bk_label_dct[code][j]['label_bk_kMacd_trd_1'].setText('None')
                            self.per_bk_label_dct[code][j]['label_bk_trend3_1'].setText('None')
                            self.per_bk_label_dct[code][j]['label_bk_beili_1'].setText('None')
                        j+=1
        except Exception as e:
            print('xxxxx', e)

    def write_ids_basic_info(self, code):
        try:
            self.per_ids_widget_dct[code].label_ids_code_val.setText(str(self.trend.ids_obj_dct[code].code))
            # self.per_ids_widget_dct[code].label_ids_name_val.setText(str(self.trend.ids_obj_dct[code].name))

            self.per_bk_label_dct[code] = {}
            j = 1
            for per_bk in self.trend.bk_code[self.trend.bk_code['code'] == code].itertuples():
                try:
                    self.per_bk_label_dct[code][j] = {}

                    self.per_bk_label_dct[code][j]['label_bk_code_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_code_1'].setObjectName("label_bk_code_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_code_1'], j, 0, 1, 1)

                    self.per_bk_label_dct[code][j]['label_bk_name_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_name_1'].setObjectName("label_bk_name_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_name_1'], j, 1, 1, 1)

                    self.per_bk_label_dct[code][j]['label_bk_kMacd_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_kMacd_1'].setObjectName("label_bk_kMacd_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_kMacd_1'], j, 2, 1, 1)

                    self.per_bk_label_dct[code][j]['label_bk_kDif_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_kDif_1'].setObjectName("label_bk_kDif_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_kDif_1'], j, 3, 1, 1)

                    self.per_bk_label_dct[code][j]['label_bk_kMacd_trd_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_kMacd_trd_1'].setObjectName("label_bk_kMacd_trd_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_kMacd_trd_1'], j, 4, 1, 1)

                    self.per_bk_label_dct[code][j]['label_bk_trend3_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_trend3_1'].setObjectName("label_bk_trend3_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_trend3_1'], j, 5, 1, 1)

                    self.per_bk_label_dct[code][j]['label_bk_beili_1'] = QtWidgets.QLabel(self.per_ids_widget_dct[code].gridLayoutWidget_2)
                    self.per_bk_label_dct[code][j]['label_bk_beili_1'].setObjectName("label_bk_beili_1")
                    self.per_ids_widget_dct[code].gridLayout_2.addWidget(self.per_bk_label_dct[code][j]['label_bk_beili_1'], j, 6, 1, 1)

                except Exception as e:
                    print('@@@@@@@@@', e)

                j += 1

            self.per_ids_widget_dct[code].gridLayoutWidget_2.setGeometry(QtCore.QRect(1060, 40, 480, 18*j))
        except Exception as e:
            print('yyyyyyy', e)
class kline(FigureCanvas):
    def __init__(self, trend, code):
        self.trend = trend
        self.code = code
        self.trend_one = self.trend.ids_obj_dct[self.code]
        self.fig = Figure()
        self.axes = self.fig.add_axes([0.08, 0.18, 0.88, 0.8])
        # self.axes = self.fig.add_subplot(111)
        super(kline, self).__init__(self.fig)

    def draw_kline(self):
        try:
            if len(self.trend_one.m_price_dct['p']) == 0:
                return 0
            self.axes.clear()

            # 设置y轴坐标
            y_min_price = self.trend_one.today_lowest * 0.99
            y_max_price = self.trend_one.today_highest * 1.01

            yticks_lst = []
            for y in range(int(y_min_price*100), int(y_max_price*100), 10):
                yticks_lst.append(y/100)
            self.axes.set_ylim(y_min_price, y_max_price)

            # 设置x轴坐标
            xlim_min = 0
            xlim_max = self.trend_one.m_price_dct['x'][-1] + 100
            self.axes.set_xlim(xlim_min, xlim_max)
            step = math.ceil(xlim_max/20)
            xticks_lst = []
            xticks_labels_lst = []
            for tick in range(xlim_min, xlim_max, step):
                xticks_lst.append(tick)
                if tick >= len(self.trend_one.m_price_dct['x']):
                    xticks_labels_lst.append('')
                else:
                    xticks_labels_lst.append(self.trend_one.m_price_dct['t'][tick])

            self.axes.set_xticks(xticks_lst)
            self.axes.set_yticks(yticks_lst)
            self.axes.set_xticklabels(xticks_labels_lst, rotation=30, fontsize='small')
            self.axes.set_yticklabels(yticks_lst)

            # 绘制所有分时线
            self.axes.plot(self.trend_one.m_price_dct['x'], self.trend_one.m_price_dct['p'], linewidth=0.8, c='gray')

            # 绘制最低价格之后的短线
            if len(self.trend_one.from_low_up_dct.keys()) > 0:
                 for per_dct in self.trend_one.from_low_up_dct.values():
                    self.axes.plot(per_dct['x'], per_dct['p'], linewidth=1, c='red')

            # 绘制价格文本
            self.axes.text(self.trend_one.m_price_dct['x'][-1], self.trend_one.m_price_dct['p'][-1], self.trend_one.m_price_dct['p'][-1])
            self.axes.plot([0, self.trend_one.m_price_dct['x'][-1]], [self.trend_one.m_price_dct['p'][-1], self.trend_one.m_price_dct['p'][-1]], linestyle=':', linewidth=0.5, c='gray')

            # 绘制买点
            if len(self.trend_one.buy_point['p']) > 0:
                self.axes.scatter(self.trend_one.buy_point['x'], self.trend_one.buy_point['p'], c='red')

            self.draw()
        except Exception as e:
            print(e)
            raise e


def drop_coll(run_type, today, yestoday):
    # 获取当前执行文件的名称
    file_name = str(os.path.basename(__file__).split('.')[0])
    # 连接mongoDB
    myclient = pm.MongoClient("mongodb://localhost:27017/")
    fd = myclient["freedom"]

    fd.drop_collection(file_name + '_trd_rec')
    # fd.drop_collection(file_name + '_beili')
    fd.drop_collection(file_name + '_trd_has_buy')
    fd.drop_collection(file_name + '_noSale_money')
    fd.drop_collection(file_name + '_bk_ids')

    path = './' + file_name + '_log.xlsx'  # 文件路径
    if os.path.exists(path):  # 如果文件存在
        os.remove(path)
    # path = './' + file_name + '_beili.xlsx'  # 文件路径
    # if os.path.exists(path):  # 如果文件存在
    #     os.remove(path)
    path = './' + file_name + '_hasbuy.xlsx'  # 文件路径
    if os.path.exists(path):  # 如果文件存在
        os.remove(path)
    path = './' + file_name + '_trade.xlsx'  # 文件路径
    if os.path.exists(path):  # 如果文件存在
        os.remove(path)


def trade_report(run_type):
    if run_type == 'backtest':
        # 完成回测后，导出数据情况：
        file_name = str(os.path.basename(__file__).split('.')[0])
        trend = Trend('2021-01-04', '2020-12-31', run_type)
        # 获取trend_rec数据
        res_trend_info = trend.trend_rec.find()
        total_trend_df = pd.DataFrame()
        for r in res_trend_info:
            r.pop('_id')
            total_trend_df = total_trend_df.append(r, ignore_index=True)

        total_trend_df.to_excel('./' + file_name + '_trade.xlsx')

        # 获取hasbuy买明细
        total_hasbuy_df = pd.DataFrame()
        res_hasbuy = trend.hasBuy.find()
        for r in res_hasbuy:
            r.pop('_id')
            total_hasbuy_df = total_hasbuy_df.append(r, ignore_index=True)

        total_hasbuy_df.to_excel('./' + file_name + '_hasbuy.xlsx')

        # 获取nosale 明细
        detail_nosale_info = pd.DataFrame()
        res_nosale = trend.fd[trend.file_name + 'noSale_money'].find()
        for r in res_nosale:
            r.pop('_id')
            detail_nosale_info = detail_nosale_info.append(r, ignore_index=True)

        detail_nosale_info.to_excel('./' + file_name + '_log.xlsx')

    elif run_type == 'trading':
        # 统计当日交易情况
        pass
    elif run_type == 'trading_debug':
        pass

def start_cp(trend):

    app = QApplication(sys.argv)
    cp_win = control_panel(trend)
    cp_win.show()
    sys.exit(app.exec_())

def main(run_type):
    if run_type == 'backtest':
        drop_coll(run_type)
        # 按交易日期寻找交易标的
        trade_date = {1: ['2021-01-04', '2021-01-05', '2021-01-06', '2021-01-07', '2021-01-08', '2021-01-11', '2021-01-12', '2021-01-13', '2021-01-14', '2021-01-15', '2021-01-18', '2021-01-19', '2021-01-20', '2021-01-21', '2021-01-22', '2021-01-25', '2021-01-26', '2021-01-27', '2021-01-28', '2021-01-29'],
                      2: ['2021-02-01', '2021-02-02', '2021-02-03', '2021-02-04', '2021-02-05', '2021-02-08', '2021-02-09', '2021-02-10', '2021-02-18', '2021-02-19', '2021-02-22', '2021-02-23', '2021-02-24', '2021-02-25', '2021-02-26'],
                      3: ['2021-03-01', '2021-03-02', '2021-03-03', '2021-03-04', '2021-03-05', '2021-03-08', '2021-03-09', '2021-03-10', '2021-03-11', '2021-03-12', '2021-03-15', '2021-03-16', '2021-03-17', '2021-03-18', '2021-03-19', '2021-03-22', '2021-03-23', '2021-03-24', '2021-03-25', '2021-03-26', '2021-03-29', '2021-03-30', '2021-03-31'],
                      4: ['2021-04-01', '2021-04-02', '2021-04-06', '2021-04-07', '2021-04-08', '2021-04-09', '2021-04-12', '2021-04-13', '2021-04-14', '2021-04-15', '2021-04-16', '2021-04-19', '2021-04-20', '2021-04-21', '2021-04-22', '2021-04-23', '2021-04-26', '2021-04-27', '2021-04-28', '2021-04-29', '2021-04-30'],
                      5: ['2021-05-06', '2021-05-07', '2021-05-10', '2021-05-11', '2021-05-12', '2021-05-13', '2021-05-14', '2021-05-17', '2021-05-18', '2021-05-19', '2021-05-20', '2021-05-21', '2021-05-24', '2021-05-25', '2021-05-26', '2021-05-27', '2021-05-28', '2021-05-31'],
                      6: ['2021-06-01', '2021-06-02', '2021-06-03', '2021-06-04', '2021-06-07', '2021-06-08', '2021-06-09', '2021-06-10', '2021-06-11', '2021-06-15', '2021-06-16', '2021-06-17', '2021-06-18', '2021-06-21', '2021-06-22', '2021-06-23', '2021-06-24', '2021-06-25', '2021-06-28', '2021-06-29', '2021-06-30'],
                      7: ['2021-07-01', '2021-07-02', '2021-07-05', '2021-07-06', '2021-07-07', '2021-07-08', '2021-07-09', '2021-07-12', '2021-07-13', '2021-07-14', '2021-07-15', '2021-07-16', '2021-07-19', '2021-07-20', '2021-07-21', '2021-07-22', '2021-07-23', '2021-07-26', '2021-07-27', '2021-07-28', '2021-07-29', '2021-07-30'],
                      8: ['2021-08-02', '2021-08-03', '2021-08-04', '2021-08-05', '2021-08-06', '2021-08-09', '2021-08-10', '2021-08-11', '2021-08-12', '2021-08-13', '2021-08-16', '2021-08-17', '2021-08-18', '2021-08-19', '2021-08-20', '2021-08-23', '2021-08-24', '2021-08-25', '2021-08-26', '2021-08-27', '2021-08-30', '2021-08-31'],
                      9: ['2021-09-01', '2021-09-02', '2021-09-03', '2021-09-06', '2021-09-07', '2021-09-08', '2021-09-09', '2021-09-10', '2021-09-13', '2021-09-14', '2021-09-15', '2021-09-16', '2021-09-17', '2021-09-22', '2021-09-23', '2021-09-24', '2021-09-27', '2021-09-28', '2021-09-29', '2021-09-30'],
                      10: ['2021-10-08', '2021-10-11', '2021-10-12', '2021-10-13', '2021-10-14', '2021-10-15', '2021-10-18', '2021-10-19', '2021-10-20', '2021-10-21', '2021-10-22', '2021-10-25', '2021-10-26', '2021-10-27', '2021-10-28', '2021-10-29'],
                      11: ['2021-11-01', '2021-11-02', '2021-11-03', '2021-11-04', '2021-11-05', '2021-11-08', '2021-11-09', '2021-11-10', '2021-11-11', '2021-11-12', '2021-11-15', '2021-11-16', '2021-11-17', '2021-11-18', '2021-11-19', '2021-11-22', '2021-11-23', '2021-11-24', '2021-11-25', '2021-11-26', '2021-11-29', '2021-11-30'],
                      12: ['2021-12-01', '2021-12-02', '2021-12-03', '2021-12-06', '2021-12-07', '2021-12-08', '2021-12-09', '2021-12-10', '2021-12-13', '2021-12-14', '2021-12-15', '2021-12-16', '2021-12-17', '2021-12-20', '2021-12-21', '2021-12-22', '2021-12-23', '2021-12-24', '2021-12-27', '2021-12-28', '2021-12-29', '2021-12-30', '2021-12-31']}

        trade_date_list = []
        for t in range(1, 13):
            trade_date_list += trade_date[t]

        # 读取所有的买点, 根据标的情况，创建交易买进程
        index = 0
        for today in trade_date_list:
            # 提取日期 today, yestoday
            if today == '2021-01-04':
                yestoday = '2020-12-31'
            else:
                yestoday = trade_date_list[index - 1]

            index += 1

            trend = Trend(today, yestoday, run_type)
            thr_dct[1] = thr.Thread(target=trend.jk_run)

            thr_dct[1].start()

    elif run_type in ['trading', 'trading_debug']:
        if run_type == 'trading_debug':

            yestoday_lst = ["2023-04-07"]
            today_lst = ["2023-04-10"]

            # yestoday_lst = ["2023-03-09", "2023-03-10", "2023-03-13","2023-03-14","2023-03-15","2023-03-16"]
            # today_lst = ["2023-03-10", "2023-03-13", "2023-03-14","2023-03-15","2023-03-16","2023-03-17"]

            for yestoday, today in zip(yestoday_lst, today_lst):
                print(yestoday, today)
                trend = Trend(today, yestoday, run_type)

                trend.jk_run_loop = asyncio.new_event_loop()

                thr_dct = {}
                thr_dct[1] = thr.Thread(target=trend.jk_run)
                thr_dct[1].start()

        elif run_type == 'trading':

            today = str(datetime.datetime.now().date())
            # today = "2023-03-31"
            trend = Trend(today, '2023-04-07', run_type)
            thr_dct = {}

            trend.jk_run_loop = asyncio.new_event_loop()

            # per_jk_list = 48
            # thr_nums = math.ceil(len(trend.all_jk_list)/per_jk_list)
            # for start in range(0, thr_nums):
            #     ncode = []
            #     for c in list(trend.all_jk_list)[start*per_jk_list:(start+1):per_jk_list]:
            #         ncode.append(trend.chg_code_type(c))
            #     all_list_2str = ','.join(ncode)
            #     url = "https://qt.gtimg.cn/q=" + all_list_2str + "&r=926277345"
            #     thr_dct[5+start] = thr.Thread(target=trend.get_ids_fromUrl, args=(url,))
            #     thr_dct[5+start].start()

            thr_dct[1] = thr.Thread(target=trend.get_bk_dp_fromUrl)
            thr_dct[2] = thr.Thread(target=trend.jk_run)
            thr_dct[3] = thr.Thread(target=trend.keep_login)
            thr_dct[4] = thr.Thread(target=trend.get_reality_cangwei)

            thr_dct[1].start()
            thr_dct[2].start()
            thr_dct[3].start()
            thr_dct[4].start()

            time.sleep(120)

    start_cp(trend)


if __name__ == '__main__':
    # 配置runtype: backtest|trading| trading_debug
    run_type = 'trading'
    main(run_type)
