# !/usr/bin python3
# -*- encoding:utf-8 -*-
# @Author : Samzhang
# @File : etf1.11.py
# @Time : 2022/1/11 6:56

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


class trend_one:
    def __init__(self, code, obj, ids_selfBk_lst):
        self.code = code

        # 获取当前执行文件的名称
        self.file_name = obj.file_name
        # 设置交易时间
        self.today = obj.today
        self.yestoday = obj.yestoday

        # 连接mongoDB
        self.myclient = pm.MongoClient("mongodb://localhost:27017/")
        self.fd = self.myclient["freedom"]

        self.run_type = obj.run_type
        self.logger = obj.logger

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
                print(self.code, bk_code, bk_kline_dif, bk_kline_dea)
                if bk_kline_dif > bk_kline_dea:
                    self.bk_cond = True


        elif self.run_type in ['trading', 'trading_debug']:

            if self.run_type == 'trading':
                self.trend_rec = self.fd['trend_rec']
                self.hasBuy = self.fd['trend_has_buy']
                self.basic_data_store = self.fd['trend_basic_data']
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

    async def jk_buy(self, lastreq, curr_time, curr_bk_info=None, curr_dp_info=None):
        t = time.time()

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
                    pct_bt = round(yingkui/r['money'], 4)
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
            return 0

        # 如果最近一条记录是top，那么跳过买动作
        if self.isAppear_top:
            # print(self.code, 'return 5')
            return 0

        # 如果你是底部，但是现在价格低于了这个底部，那就不能买，应该全卖出
        if self.yes1_lowest > self.curr_price:
            return 0

        # 判断大盘和板块退出条件
        # ***************************************************************************
        # 如果大盘和行业板块都不好则退出，trading结合当日curr_price, backtest结合昨日kline
        if self.run_type == 'trading' or self.run_type == 'trading_debug':
            # 获取板块情况
            self.bk_cond = False
            self.bk_code_lst = self.bk_code[self.bk_code['code'] == self.code]['bk_code'].values
            for bk_code in self.bk_code_lst:
                # print(bk_code, bk_code in curr_bk_info.keys(), end=',')

                if bk_code in curr_bk_info.keys():
                    # print(bk_code, self.bk_cond, self.code, curr_bk_info[bk_code]['curr_kMacd'], curr_bk_info[bk_code]['curr_kMacd_trend'],
                    #       curr_bk_info[bk_code]['min_macd'],curr_bk_info[bk_code]['min_macd_trend'],curr_bk_info[bk_code]['min_dif'],curr_bk_info[bk_code]['min_dea'])
                    if (curr_bk_info[bk_code]['curr_kMacd'] > 0 and curr_bk_info[bk_code]['curr_kMacd_trend'] == 'incs') and \
                        ((curr_bk_info[bk_code]['min_macd'] > 0 and curr_bk_info[bk_code]['min_macd_trend'] == 'incs' and curr_bk_info[bk_code]['min_dif'] > 0) or curr_bk_info[bk_code]['price_trend'] == 'incs'):
                        self.bk_cond = True
                        # 有一个板块满足条件，就退出板块循环
                        break
                        # print(bk_code, self.bk_cond, self.code, curr_bk_info[bk_code]['curr_kMacd'], curr_bk_info[bk_code]['curr_kMacd_trend'],
                        #       curr_bk_info[bk_code]['min_macd'], curr_bk_info[bk_code]['min_macd_trend'], curr_bk_info[bk_code]['min_dif'], curr_bk_info[bk_code]['min_dea'], curr_bk_info[bk_code]['price_trend'])

                    # 如果已经购买第一次，第二次购买的话，放宽板块购买条件，只要板块没有走差，即可进行补仓
                    if self.bt1_cost != 0 and (curr_bk_info[bk_code]['curr_kMacd'] > 0 and curr_bk_info[bk_code]['curr_kMacd_trend'] != 'decs'):
                        self.bk_cond = True
                        # print(bk_code, self.bk_cond, self.code, curr_bk_info[bk_code]['curr_kMacd'], curr_bk_info[bk_code]['curr_kMacd_trend'], self.bt1_cost)
                        break

            if self.bk_cond == False:
                return 0

        elif self.run_type == 'backtest':
            # 大盘dif<dea,或者顶背离，或者下跌趋势，且板块dif<dea那么退出
            if ((self.sh_yestoday['dif1'].values[0] < self.sh_yestoday['dea1'].values[0]) or
                (self.sz_yestoday['dif1'].values[0] < self.sz_yestoday['dea1'].values[0]) or
                self.dapan_yes1_beili == 'top' or self.dapan_xiadie_trend) and self.bk_cond == False:
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
            ids_curr_kMacd_trd = self.mk(ids_kline_macd[-3:])[0]

            if self.bt1_cost == 0 and not (ids_curr_kMacd > 0 and ids_curr_kMacd_trd == 'incs'):
                return 0
            elif self.bt1_cost != 0 and (ids_curr_kMacd < 0 or ids_curr_kMacd_trd == 'decs'):
                return 0

            # 计算个股分时macd情况
            ids_mDif_lst, ids_mDea_lst, ids_mDw_lst = talib.MACD(np.array(self.buy_min_dct['curr_price']), 28, 68, 18)
            ids_mDif, ids_mDea, ids_mDw = ids_mDif_lst[-1], ids_mDea_lst[-1], ids_mDw_lst[-1]
            ids_mMacd_lst = (ids_mDif_lst - ids_mDea_lst) * 2
            ids_mMacd = ids_mMacd_lst[-1]
            if len(ids_mMacd_lst[-28:]) >= 3:
                ids_mMacd_trd = self.mk(ids_mMacd_lst[-28:])[0]
            else:
                ids_mMacd_trd = None

            # 开盘macd还未计算出来时，使用price_trend
            price_trend = ''
            # if curr_time.hour == 9 and curr_time.minute <= 45:
            if np.isnan(ids_mMacd) and len(self.buy_min_dct['curr_price']) >= 28:
                price_trend = self.mk(self.buy_min_dct['curr_price'])[0]

            self.buy_cond1 = ((self.bt1_cost == 0 and (ids_curr_kMacd > 0 and ids_curr_kMacd_trd == 'incs')) or (self.bt1_cost != 0 and (ids_curr_kMacd > 0 and ids_curr_kMacd_trd != 'incs'))) and ((ids_mMacd > 0 and ids_mDif > 0 and ids_mMacd_trd == 'incs') or (price_trend == 'incs'))

            if self.buy_cond1 == False:
                return 0

            # 查询是否已经买入过
            self.hasBuy_count = self.hasBuy.count_documents({'code': self.code, 'buy_date': self.today})
            luocha = 0.0046

            if self.hasBuy_count == 0 and (not ((self.today_highest-self.today_lowest)/self.today_lowest >= 0.036 and (self.today_highest - self.curr_price)/self.curr_price <= 0.008)):
                self.buy_cond4 = True
            elif self.hasBuy_count == 1:
                if len(self.catch_lowest) > 3:
                    lowest_trend = self.mk(self.catch_lowest)[0]
                    print(f"{self.code}, lowest_trend:{lowest_trend}")
                    self.buy_cond4 = (lowest_trend == 'incs') and (self.catch_lowest.iloc[0] == self.today_lowest) and (self.catch_lowest.max() == self.catch_lowest.iloc[-1]) and 0.003 <= (self.catch_lowest.iloc[-1] - self.today_lowest) / self.today_lowest
            else:
                self.buy_cond4 = False

            print(f"{curr_time} {self.code}, cond1:{self.buy_cond1}, cond4:{self.buy_cond4}, kMacd:{ids_curr_kMacd}, kMacd_trd:{ids_curr_kMacd_trd}, mMacd:{ids_mMacd}, mDif:{ids_mDif}, mMacd_trd:{ids_mMacd_trd}, price_trd:{price_trend}")

            if self.buy_cond1 and self.buy_cond4:
                self.act = 'buy'
                # 更新all_cangwei
                self.update_all_cangwei()

                # 因为整体仓位达到上限，导致不能交易
                if self.all_cangwei >= self.allow_all_cangwei:
                    print("self.all_cangwei >= self.allow_all_cangwei",self.all_cangwei, self.allow_all_cangwei)
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
                    self.money1 = 0
                    self.money2 = self.per_top_money * 2
                    self.money3 = self.per_top_money * 2
                    self.money4 = self.per_top_money * 4
                    self.money5 = 0
                else:
                    self.money1 = self.per_top_money * 2
                    self.money2 = self.per_top_money * 3
                    self.money3 = self.per_top_money * 6
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
                    ids_bk_buy_flag = False
                    for bk_code in self.ids_selfBk_lst:
                        bk_ids_r = self.bk_ids.find_one({'date': self.today, 'bk_code': bk_code})
                        if bk_ids_r:
                            if bk_ids_r['hasBuy_nums'] < bk_ids_r['idsNums_allowed_buy'] and self.code not in bk_ids_r['hasBuy_lst']:
                                ids_bk_buy_flag = True
                                new_hasBuy_lst = bk_ids_r['hasBuy_lst'] + [self.code]
                                new_hasBuy_nums = bk_ids_r['hasBuy_nums'] + 1
                                self.bk_ids.update_one({'date': self.today, 'bk_code': bk_code}, {'$set': {'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums}})
                                print('bk_ids update:', {'code:': self.code, 'bk_code': bk_code, 'hasBuy_lst': new_hasBuy_lst, 'hasBuy_nums': new_hasBuy_nums})
                    if ids_bk_buy_flag == False:
                        print(self.code, 'return')
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
                                            self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                    except Exception as e:
                                        self.logger.error(str(e) + ' 返回结果：' + str(res))
                            elif self.run_type in ['trading_debug', 'backtest']:
                                res = 'backtest'
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})

                            self.logger.info(self.code + ' bt1买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))
                        else:
                            self.hasBuy.insert_one(
                                {'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                        self.bt1_cost = self.cost
                        self.catch_lowest = pd.Series(dtype='float64')

                    except Exception as e:
                        self.logger.error(f"error: {self.code}, {e}")

                elif self.hasBuy_count == 1 and self.act == 'buy':

                    # 判断间隔时间
                    try:
                        res = self.hasBuy.find_one({'code': self.code, 'buy_date': self.today, 'bt': 1})
                        t1 = datetime.datetime.strptime(res['time'][0:19], '%Y-%m-%d %H:%M:%S')
                        # print("(curr_time - t1).total_seconds()", (curr_time - t1).total_seconds())
                        # print("(res['price'] - self.curr_price) / res['price']:", (res['price'] - self.curr_price) / res['price'])
                        # print("(res['price'] - self.curr_price) / res['price'] >= luocha", (res['price'] - self.curr_price) / res['price'] >= luocha)
                        if (curr_time - t1).total_seconds() > 480 and (res['price'] - self.curr_price) / res['price'] >= luocha:

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
                                                self.hasBuy.insert_one(
                                                    {'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                        except Exception as e:
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                    res = 'backtest'
                                self.logger.info(self.code + ' bt2买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))

                            else:
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})

                            self.catch_lowest = pd.Series(dtype='float64')
                    except Exception as e:
                        self.logger.error(f"error: {self.code}, {e}")

                elif self.hasBuy_count == 2 and self.act == 'buy':

                    self.money = self.money3

                    if self.money == 0:
                        return 0

                    # 判断间隔时间
                    try:
                        res = self.hasBuy.find_one({'code': self.code, 'buy_date': self.today, 'bt': 2})
                        t1 = datetime.datetime.strptime(res['time'][0:19], '%Y-%m-%d %H:%M:%S')

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
                                                self.hasBuy.insert_one(
                                                    {'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                        except Exception as e:
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                elif self.run_type in ['trading_debug', 'backtest']:

                                    self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                    res = 'backtest'

                                self.logger.info(self.code + ' bt3买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))

                            else:
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})

                            self.catch_lowest = pd.Series(dtype='float64')
                    except Exception as e:
                        self.logger.error(f"error: {self.code}, {e}")

                elif self.hasBuy_count == 3 and self.act == 'buy':

                    self.money = self.money4

                    if self.money == 0:
                        return 0

                    # 判断间隔时间
                    try:
                        res = self.hasBuy.find_one({'code': self.code, 'buy_date': self.today, 'bt': 3})
                        t1 = datetime.datetime.strptime(res['time'][0:19], '%Y-%m-%d %H:%M:%S')

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

                            bt = 4
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
                                                self.hasBuy.insert_one(
                                                    {'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                        except Exception as e:
                                            self.logger.error(str(e) + ' 返回结果：' + str(res))

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})
                                    res = 'backtest'

                                self.logger.info(self.code + ' bt4买入, 单价:' + str(self.buy_price) + ' 数量:' + str(self.num) + ' 返回结果：' + str(res))
                            else:
                                self.hasBuy.insert_one({'code': self.code, 'price': self.curr_price, 'time': str(curr_time), 'buy_date': self.today, 'bt': self.bt, 'money': self.buyMoney, 'isSold': 0, 'cost': self.cost, 'num': self.num, 'pct_bt': 0, 'yingkui': 0})

                            self.catch_lowest = pd.Series(dtype='float64')
                    except Exception as e:
                        self.logger.error(f"error: {self.code}, {e}")

        except Exception as e:
            raise e
            self.logger.error(self.code + ' error :' + str(e))

    async def jk_sale(self, lastreq, curr_time, curr_bk_info=None, curr_dp_info=None):

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
        ids_curr_kDif_trd = self.mk(ids_kline_dif[-3:])[0]
        ids_curr_kDea_trd = self.mk(ids_kline_dea[-3:])[0]
        ids_curr_kMacd_trd = self.mk(ids_kline_macd[-3:])[0]

        # 计算个股分时macd情况
        ids_mDif_lst, ids_mDea_lst, ids_mDw_lst = talib.MACD(np.array(self.sold_min_dct['curr_price']), 28, 68, 18)
        ids_mDif, ids_mDea, ids_mDw = ids_mDif_lst[-1], ids_mDea_lst[-1], ids_mDw_lst[-1]
        ids_mMacd_lst = (ids_mDif_lst - ids_mDea_lst) * 2
        ids_mMacd = ids_mMacd_lst[-1]
        if len(ids_mMacd_lst[-28:]) >= 3:
            ids_mMacd_trd = self.mk(ids_mMacd_lst[-28:])[0]
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
                else:
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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, 'yingkui': yingkui, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:

                                    res = self.run_type
                                    self.logger.info(f"sold_point1: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, 'yingkui': yingkui, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui}})

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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                self.logger.info(f"sold_point2: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error4: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error4', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    elif (sold_type[0:2] == "xt" or sold_type == "top_beili") and ((self.catch_highest.max() - self.catch_highest[-1]) / self.catch_highest.max() >= 0.003) and sold_trend_cond:
                        if r['st'] == 0:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 5000):
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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error9: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # # send_notice('Sold_Error9', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 1 and (curr_price - r['soldPrice1']) / r['soldPrice1'] >= 0.003:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 5000):
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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})


                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': r['st'] + 1, f"soldTime{r['st'] + 1}": str(curr_time), f"soldPrice{r['st'] + 1}": curr_price, f"sold_type{r['st'] + 1}": sold_type, f"yingkui{r['st'] + 1}": curr_yingkui, 'yingkui': yingkui}})

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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'left_num': 0, 'st': 1, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'sold_type1': sold_type, 'yingkui1': yingkui, 'yingkui': yingkui}})

                                self.logger.info(f"sold_point4: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error8: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error8', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                    elif (r['highest_price'] - curr_price) / r['highest_price'] > 0.006 and curr_price > r['cost'] and sold_trend_cond and sold_kline_macd_cond:
                        if r['st'] == 0:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 5000):
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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 1, 'left_num': left_num, 'left_num1': left_num, 'soldNum1': num, 'soldTime1': str(curr_time), 'soldPrice1': curr_price, 'yingkui': yingkui, 'yingkui1': yingkui1, 'sold_type1': sold_type}})

                                self.logger.info(f"sold_point5: {self.code}, curr_price:{curr_price}, num:{num}, response:{res}")

                            except Exception as e:
                                self.logger.error(f"Sold_Error9: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")  # send_notice('Sold_Error9', f"Sold_Error1: {self.code}, time:{curr_time}, sold_type:{sold_type}, e:{e}")

                        elif r['st'] == 1 and (curr_price - r['soldPrice1']) / r['soldPrice1'] >= 0.003:
                            try:
                                if r['left_num'] < 300 or (r['left_num'] * r['price'] < 5000):
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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': isSold, 'st': 2, 'left_num': left_num, 'left_num2': left_num, 'soldNum2': num, 'soldTime2': str(curr_time), 'soldPrice2': curr_price, 'yingkui': yingkui, 'yingkui2': yingkui2, 'sold_type2': sold_type}})

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
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

                                elif self.run_type in ['trading_debug', 'backtest']:
                                    res = self.run_type

                                    if is_sale_success:
                                        self.trend_rec.update_one(condition, {
                                            '$set': {'isSold': 1, 'st': 3, 'left_num': left_num, 'left_num3': left_num, 'soldNum3': num, 'soldTime3': str(curr_time), 'soldPrice3': curr_price, 'yingkui': yingkui, 'yingkui3': yingkui3, 'sold_type3': sold_type}})

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

    def mk(self, x, alpha=0.5):  # 0<alpha<0.5 1-alpha/2为置信度
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

    def get_price_trend(self, curr_price_lst):
        if np.isnan(curr_price_lst[-30:]).sum() == 0 and len(curr_price_lst[-30:]) >= 3:
            min1_price_trend = self.mk(curr_price_lst[-30:], 0.5)
        else:
            min1_price_trend = (None, 0)

        if np.isnan(curr_price_lst[-60:]).sum() == 0 and len(curr_price_lst[-60:]) >= 3:
            min3_price_trend = self.mk(curr_price_lst[-60:], 0.5)
        else:
            min3_price_trend = (None, 0)

        if np.isnan(curr_price_lst[-90:]).sum() == 0 and len(curr_price_lst[-120:]) >= 3:
            min6_price_trend = self.mk(curr_price_lst[-120:], 0.5)
        else:
            min6_price_trend = (None, 0)
        return (min1_price_trend, min3_price_trend, min6_price_trend)

    def get_macd_trend(self, macd_lst):
        # print('macd_lst[-17] len:', len(macd_lst[-17:]), ' nan.sum:', np.isnan(macd_lst[-17:]).sum(), ' not nan:', len(macd_lst[-17:])-np.isnan(macd_lst[-17:]).sum())
        if np.isnan(macd_lst[-30:]).sum() == 0 and len(macd_lst[-30:]) >= 3:
            min1_macd_trend = self.mk(macd_lst[-30:], 0.5)
        else:
            min1_macd_trend = (None, 0)
        if np.isnan(macd_lst[-60:]).sum() == 0 and len(macd_lst[-60:]) >= 3:
            min3_macd_trend = self.mk(macd_lst[-60:], 0.5)
        else:
            min3_macd_trend = (None, 0)
        if np.isnan(macd_lst[-90:]).sum() == 0 and len(macd_lst[-120:]) >= 3:
            min6_macd_trend = self.mk(macd_lst[-120:], 0.5)
        else:
            min6_macd_trend = (None, 0)
        return (min1_macd_trend, min3_macd_trend, min6_macd_trend)

    def get_dif_trend(self, dif_lst):
        if np.isnan(dif_lst[-30:]).sum() == 0 and len(dif_lst[-30:]) >= 3:
            min1_dif_trend = self.mk(dif_lst[-30:], 0.5)
        else:
            min1_dif_trend = (None, 0)
        if np.isnan(dif_lst[-60:]).sum() == 0 and len(dif_lst[-60:]) >= 3:
            min3_dif_trend = self.mk(dif_lst[-60:], 0.5)
        else:
            min3_dif_trend = (None, 0)
        if np.isnan(dif_lst[-90:]).sum() == 0 and len(dif_lst[-120:]) >= 3:
            min6_dif_trend = self.mk(dif_lst[-120:], 0.5)
        else:
            min6_dif_trend = (None, 0)
        return (min1_dif_trend, min3_dif_trend, min6_dif_trend)

    def get_dea_trend(self, dea_lst):
        if np.isnan(dea_lst[-30:]).sum() == 0 and len(dea_lst[-30:]) >= 3:
            min1_dea_trend = self.mk(dea_lst[-30:], 0.5)
        else:
            min1_dea_trend = (None, 0)
        if np.isnan(dea_lst[-60:]).sum() == 0 and len(dea_lst[-60:]) >= 3:
            min3_dea_trend = self.mk(dea_lst[-60:], 0.5)
        else:
            min3_dea_trend = (None, 0)
        if np.isnan(dea_lst[-90:]).sum() == 0 and len(dea_lst[-120:]) >= 3:
            min6_dea_trend = self.mk(dea_lst[-120:], 0.5)
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
    def __init__(self, today, yestoday, run_type, jk_buy_lst=None):

        self.run_type = run_type

        # 设置交易时间
        self.today = today
        self.yestoday = yestoday

        # 连接mongoDB
        self.myclient = pm.MongoClient("mongodb://localhost:27017/")
        self.fd = self.myclient["freedom"]

        # 获取当前执行文件的名称
        self.file_name = str(os.path.basename(__file__).split('.')[0])

        # 启动日志
        # self.logger = Logger('./trading_' + str(self.today) + '.log').get_logger()
        self.logger = self.getlogger()

        # 设置ip池
        self.ipPool = self.fd['ipPool']
        self.allIpPool = self.fd['allIpPool']

        # 设置板块队列池
        self.fh_bk = open(f'./bk_info_{today}.txt', 'a+')
        # self.fh_alive_gbd = open(f'./alive_gbd_{today}.txt', 'a+')
        # self.fh_alive_gbi = open(f'./alive_gbi_{today}.txt', 'a+')

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

        self.bk_name = pd.read_excel('./bk_info/bk_name.xlsx')
        self.bk_code = pd.read_excel('./bk_info/bk_code.xlsx')

        if self.run_type == 'trading':
            self.data_path = './find_trend/k_daily'
        elif self.run_type == 'trading_debug':
            self.data_path = f'./find_trend/k_daily{self.yestoday}'
        elif self.run_type == 'backtest':
            self.data_path = f'./find_trend/backtest_k_daily4'

        for per in self.bk_name.itertuples():
            try:
                # 获取后，直接在此一次性颠倒数据
                if self.run_type in ['trading', 'trading_debug']:
                    try:
                        if os.path.exists(f'{self.data_path}/bk_kline/{per.code}.xlsx'):
                            self.bk_kline[per.code] = pd.read_excel(f'{self.data_path}/bk_kline/{per.code}.xlsx').iloc[::-1]
                            self.bk_close_lst[per.code] = copy.deepcopy(self.bk_kline[per.code]['close'].values)
                    except Exception as e:
                        self.logger.error(e)
                elif self.run_type == 'backtest':
                    if os.path.exists(f'{self.data_path}/bk_kline/{per.code}.xlsx'):
                        temp_bk_df = pd.read_excel(f'{self.data_path}/bk_kline/{per.code}.xlsx').iloc[::-1]

                    int_yestoday = int(self.yestoday[0:4] + self.yestoday[5:7] + self.yestoday[8:])
                    self.bk_kline[per.code] = copy.deepcopy(temp_bk_df[temp_bk_df['trade_date_int'] <= int_yestoday])
                    print(self.bk_kline[per.code].tail(1)[['name', 'trade_date', 'trade_date_int', 'dif1', 'dea1']])

                self.bk_chg_pct_dct[per.code] = []
                self.bk_chg_green_dct[per.code] = []
                self.bk_min_price_dct[per.code] = []

            except Exception as e:
                raise e
                continue

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

        if self.run_type == 'backtest':
            self.trend_rec = self.fd[self.file_name + 'trend_rec']
            self.hasBuy = self.fd[self.file_name + 'trend_has_buy']
            self.basic_data_store = self.fd[self.file_name + 'trend__basic_data']
            self.trend_reality = self.fd[self.file_name + 'trend_reality']
            self.bk_ids = self.fd[self.file_name + '_bk_ids']

            # 准备买入卖出标的
            if jk_buy_lst != None:
                self.all_jk_list = jk_buy_lst
            else:
                self.all_jk_list = []

            # 获取当日筛选出来的趋势股数量，作为当日的行情指标
            self.trend_nums = len(self.all_jk_list)

            self.all_jk_buy_list = copy.deepcopy(self.all_jk_list)
            self.all_jk_sale_list = []
            # 加入未卖出标的
            for r in self.trend_rec.find({'isSold': 0}):
                # print(r['code'])
                self.all_jk_sale_list.append(r['code'])
                self.all_jk_list.append(r['code'])  # print(222,self.all_jk_list)  # if r['code'] in self.all_jk_buy_list:  #     self.all_jk_buy_list.remove(r['code'])

            # print(111, self.all_jk_list)
            self.all_jk_buy_list = self.all_jk_buy_list
            # all_jk_sale_list 可能有多日未卖出，需要去重
            self.all_jk_sale_list = set(self.all_jk_sale_list)

            # self.all_jk_list = set(self.all_jk_list)
            self.all_jk_list = set(list(self.all_jk_buy_list) + list(self.all_jk_sale_list))
            # print(333, self.all_jk_list)

            # all_jk_list出来后，准备self.all_trend_df
            # self.all_trend_df = pd.DataFrame()

            # for code in self.all_jk_list:
            #     # 将ma情况写入到对象中
            #     if not os.path.exists('./back_test/k_daily/' + self.chg_code_type(code) + '.xlsx'):
            #         print('不存在', code)
            #         continue
            #     code_df = pd.read_excel('./back_test/k_daily/' + self.chg_code_type(code) + '.xlsx')
            #     today_int = int(f"{today[0:4]}{today[5:7]}{today[8:]}")
            #     self.all_trend_df = self.all_trend_df.append(code_df[code_df['trade_date'] == today_int], ignore_index=True)

            self.all_trend_df = pd.read_excel(f'./find_trend/backtest_k_daily4/trend_k_list/{self.today}.xlsx')

            for per in self.all_trend_df.itertuples():
                self.all_trend_df.loc[per.Index, 'ts_code'] = self.chg_code_type(per.ts_code)

        elif self.run_type in ['trading', 'trading_debug']:
            if self.run_type == 'trading':
                self.trend_rec = self.fd['trend_rec']
                self.hasBuy = self.fd['trend_has_buy']
                self.basic_data_store = self.fd['trend__basic_data']
                self.trend_reality = self.fd['trend_reality']
                self.bk_ids = self.fd['trend_bk_ids']
                # 执行自检， 创建selenium句柄
                try:
                    self.trader = Auto_trade(False)
                    self.selfcheck = selfcheck.check_cangwei(self.trader)
                    self.selfcheck.update_mongo()
                except Exception as e:
                    self.logger.error(e)
            elif self.run_type == 'trading_debug':
                self.trend_rec = self.fd[self.file_name + '_trd_rec']
                self.hasBuy = self.fd[self.file_name + '_trd_has_buy']
                self.basic_data_store = self.fd[self.file_name + '_trd__basic_data']
                self.trend_reality = self.fd[self.file_name + '_trd_reality']
                self.bk_curr_rec = self.fd[self.file_name + '_bk_curr_rec' + self.today]
                self.bk_ids = self.fd[self.file_name + '_bk_ids']

            # 获取 当日 jk_ids 代码
            self.all_trend_df = pd.read_excel(f'{self.data_path}/000trend_k_list.xlsx')
            self.all_jk_buy_df = pd.read_excel(f'{self.data_path}/jk_ids.xlsx')
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

            # self.all_jk_buy_list = random.sample(self.all_jk_buy_list, 28)
            # self.all_jk_buy_list = self.all_jk_buy_list
            # all_jk_sale_list 可能有多日未卖出，需要去重
            self.all_jk_sale_list = set(self.all_jk_sale_list)

            # self.all_jk_list = set(self.all_jk_list)
            # 合并新的买和卖标的
            self.all_jk_list = set(list(self.all_jk_buy_list) + list(self.all_jk_sale_list))

            # 获取当日 jk_bk 代码
            self.bk_jk_df = pd.read_excel(f'{self.data_path}/jk_bk.xlsx')
            self.bk_jk_lst = self.bk_jk_df['ts_code'].values
            # 获取当日bk所含ids 数量，ids代码，以及ids具体数据
            self.bk_ids_1v1_df = pd.read_excel(f'{self.data_path}/bk_ids_1v1.xlsx')
            self.bk_ids_dct = {}
            hasBuy_res = self.hasBuy.find({'buy_date': self.today})
            for per_bk in self.bk_jk_df.itertuples():
                bk_ids_lst = [self.chg_code_type(str(c).zfill(6)) for c in self.bk_ids_1v1_df[self.bk_ids_1v1_df['bk_code'] == per_bk.ts_code]['ts_code'].values]
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

                if self.bk_ids.count_documents({'bk_code': per_bk.ts_code, 'date': self.today}) == 0:
                    self.bk_ids.insert_one({'date': self.today, 'bk_code': per_bk.ts_code, 'bk_name': bk_name, 'ids_nums': bk_ids_nums, 'idsNums_allowed_buy': idsNums_allowed_buy, 'idsNums_allowed_first': idsNums_allowed_buy, 'hasBuy_nums':hasBuy_nums, 'hasBuy_lst':hasBuy_lst,'bk_ids_lst': bk_ids_lst})
                else:
                    bk_ids_res = self.bk_ids.find_one({'date': self.today, 'bk_code': per_bk.ts_code})
                    for r in hasBuy_res:
                        if (r['code'] in bk_ids_lst) and (r['code'] not in bk_ids_res['hasBuy_lst']):
                            hasBuy_lst = bk_ids_res['hasBuy_lst'] + [r['code']]
                            hasBuy_nums = bk_ids_res['hasBuy_nums'] + 1
                            self.bk_ids.update_one({'date': self.today, 'bk_code': per_bk.ts_code}, {'$set': {'hasBuy_nums': hasBuy_nums, 'hasBuy_lst': hasBuy_lst}})

                self.bk_ids_dct[per_bk.ts_code] = {'bk_name': bk_name, 'ids_nums': bk_ids_nums, 'idsNums_allowed_buy': idsNums_allowed_buy, 'idsNums_allowed_first': idsNums_allowed_buy, 'hasBuy_nums': hasBuy_nums, 'hasBuy_lst': hasBuy_lst, 'bk_ids_lst': bk_ids_lst}



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
                self.code_ma[j] = self.get_ma(j)
                # print(j, self.code_ma[j]['beili'], self.code_ma[j]['trd_days'], self.code_ma[j]['trend3'])
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
        self.allow_all_cangwei = 360000
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

        self.per_top_money = self.allow_all_topTradeMoney / 40

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

            # self.logger.debug(f"{self.code} self.all_cangwei:{self.all_cangwei}, self.per_top_money:{self.per_top_money}")

        except Exception as e:
            self.logger.error(e)

    def getlogger(self):
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
            fh = logging.FileHandler(filename='./trading_' + str(self.today) + '.log', encoding="utf8")
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
                self.trader = Auto_trade(False)
                time.sleep(5)

            if curr_time.hour >= 15 and curr_time.minute > 3:
                break

    def mk(self, x, alpha=0.5):  # 0<alpha<0.5 1-alpha/2为置信度
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

    # 获取基础成交数据
    def get_basic_data(self):
        # 实列化code对象
        buy_obj = {}
        for code in self.all_jk_list:
            try:
                ids_selfBk_lst = self.bk_ids_1v1_df[self.bk_ids_1v1_df['ts_code'] == code]['bk_code'].values
                buy_obj[code] = trend_one(code, self, ids_selfBk_lst)
                # self.logger.info(f'实列化{code},成功:{buy_obj[code]}')
                print(f'实列化{code},成功', end='; ')
                print(ids_selfBk_lst)
            except Exception as e:
                raise e
                self.logger.error(f'实列化{code},失败:{e}')

        id = 0
        if self.run_type == 'trading_debug':

            dp_fh = open(f'{self.data_path}/dp_{self.yestoday}.txt', 'r')
            bk_fh = open(f'{self.data_path}/bk_{self.yestoday}.txt', 'r')
            ids_fh = open(f'{self.data_path}/ids_{self.yestoday}.txt', 'r')

            day1_jk_lst = pd.read_excel(f'./find_trend/k_daily2023-02-07/jk_ids.xlsx')['ts_code'].values
            day1_jk_lst = [self.chg_code_type(str(code).zfill(6)) for code in day1_jk_lst]

            day2_jk_lst = pd.read_excel(f'./find_trend/k_daily2023-02-08/jk_ids.xlsx')['ts_code'].values
            day2_jk_lst = [self.chg_code_type(str(code).zfill(6)) for code in day2_jk_lst]

            day3_jk_lst = pd.read_excel(f'./find_trend/k_daily2023-02-09/jk_ids.xlsx')['ts_code'].values
            day3_jk_lst = [self.chg_code_type(str(code).zfill(6)) for code in day3_jk_lst]

            all_txt_jk_lst_len = len(set(day1_jk_lst + day2_jk_lst + day3_jk_lst))

            # 获取筛选过的板块代码
            self.bk_jk_lst = pd.read_excel(f'{self.data_path}/jk_bk.xlsx')['ts_code'].values
            # print('self.bk_jk_lst', self.bk_jk_lst)
            # exit()
        else:
            self.bk_jk_lst = pd.read_excel(f'{self.data_path}/jk_bk.xlsx')['ts_code'].values

        while True:
            t = time.time()
            # 每秒获取逐笔交易数据
            curr_time = datetime.datetime.now()
            time_cond = (curr_time.hour >= 9 and curr_time.hour < 15) and ((curr_time.hour == 9 and curr_time.minute >= 30) or curr_time.hour >= 10)

            # 获取dp,bk数据
            if self.run_type == 'trading_debug':
                dp_res = dp_fh.readline() + dp_fh.readline() + dp_fh.readline()
                dp_info = dp_res[36:]
                curr_time = dp_res[9:28]

                if curr_time == '' or dp_info == '':
                    continue

                curr_time = datetime.datetime.strptime(curr_time, '%Y-%m-%d %H:%M:%S')

                if (curr_time.hour == 9 and curr_time.minute < 25) or (curr_time.hour == 11 and curr_time.minute > 30) or (curr_time.hour == 12):
                    continue

                with self.dp_bk_lock:
                    curr_dp_info = copy.deepcopy(self.get_dp_data(curr_time, dp_info))

                # print('dp time:', time.time()-t)
                bk_res = bk_fh.readline()
                gn_res = bk_fh.readline()

                bk_info = bk_res[36:]
                gn_info = gn_res[36:]
                curr_time = dp_res[9:28]

                curr_time = datetime.datetime.strptime(curr_time, '%Y-%m-%d %H:%M:%S')

                with self.dp_bk_lock:
                    curr_bk_info = copy.deepcopy(self.get_bk_data(curr_time, bk_info, gn_info))
                # print('bk time:', time.time()-t)

            elif self.run_type == 'trading':
                with self.dp_bk_lock:
                    curr_bk_info = copy.deepcopy(self.curr_bk_info)
                    curr_dp_info = copy.deepcopy(self.curr_dp_info)

            # 获取ids数据，并启动jk_buy和jk_sale
            if self.run_type == 'trading_debug' or (time_cond and self.run_type == 'trading'):
                t = time.time()
                # print('time:', time.time()-t)
                # 获取代码基础数据
                try:
                    ncode = []
                    lastreq = {}

                    if self.run_type == 'trading_debug':
                        all_ids_res = ''
                        while True:
                            ids_res = ids_fh.readline()
                            if ids_res.replace('\n', '') == '':
                                break
                            all_ids_res += ids_res

                        ids_info = all_ids_res[36:]
                        curr_time = all_ids_res[9:28]

                        curr_time = datetime.datetime.strptime(curr_time, '%Y-%m-%d %H:%M:%S')

                        if (curr_time.hour == 9 and curr_time.minute < 25) or (curr_time.hour == 11 and curr_time.minute > 30) or (curr_time.hour == 12):
                            continue

                        res = re.split(";", ids_info)

                    elif self.run_type == 'trading':
                        for c in self.all_jk_list:
                            # print(c)
                            ncode.append(self.chg_code_type(c))
                        all_list_2str = ','.join(ncode)

                        url = "https://qt.gtimg.cn/q=" + all_list_2str + "&r=926277345"
                        try:
                            res = Proxy_url.urlget(url)  # print(res)
                        except Exception as e:
                            self.logger.error(str(e) + ' will try again')
                            res = req.get(url)
                            print('try again:', res)
                        else:
                            res = re.split(";", res.text)

                except Exception as e:
                    # raise e
                    self.logger.error(e)  # self.logger.error(e)
                else:
                    ser = pd.Series(dtype='float64')
                    # 移除尾巴上的/n
                    res.pop()
                    for r in res:
                        info = re.split('~', r)
                        # 修改etf代码号，前面加上sz或者sh
                        for c in self.all_jk_list:
                            if info[2] in c:
                                info[2] = c
                        # 存入当次访问的基础info数据
                        lastreq[info[2]] = info
                        # 存入个股实时价格，并在后面对比最低价格
                        ser[info[2]] = float(info[3])

                    # 将基础数据存入mongog
                    # self.basic_data2mongo(id, curr_time, ser, lastreq)
                    self.ser = ser
                    self.lastreq = {}

                    # print(self.curr_bk_info)
                    # t = time.time()

                    if (not curr_bk_info) or (not curr_dp_info):
                        continue

                    for code, per in lastreq.items():
                        self.lastreq[code] = {}
                        self.lastreq[code]['name'] = per[1]
                        self.lastreq[code]['curr_price'] = float(per[3])
                        self.lastreq[code]['today_highest'] = float(per[33])
                        self.lastreq[code]['today_lowest'] = float(per[34])
                        self.lastreq[code]['pct_chg'] = float(per[32])
                        self.lastreq[code]['yestoday_close'] = float(per[4])
                        self.lastreq[code]['today_open'] = float(per[5])

                    # self.fh_alive_gbd.write('1')
                    # 创建事件循环对象
                    # sta = time.time()
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    loop = asyncio.get_event_loop()

                    tasks_sale = []
                    for code in self.all_jk_sale_list:
                        try:
                            if code in buy_obj.keys():
                                if (not curr_bk_info) or (not curr_dp_info):
                                    continue
                                tasks_sale.append(asyncio.ensure_future(buy_obj[code].jk_sale(self.lastreq[code], curr_time, curr_bk_info, curr_dp_info)))
                            else:
                                print(code, '实力化失败，没有进入异步卖出函数')
                        except Exception as e:
                            # raise e
                            self.logger.error(e)

                    if len(tasks_sale) != 0:
                        loop.run_until_complete(asyncio.wait(tasks_sale))


                    # 根据大盘情况，和板块情况，动态控制 每个板块 买入个股数量
                    # (暂时先不调整，先实现稳定盈利的重要性超过一切）
                    # try:
                    #
                    #     dp_curr_cond_bad = (self.curr_dp_info['sh']['curr_macd'] < 0 and self.curr_dp_info['sh']['curr_dif'] < 0) or (self.curr_dp_info['sh']['mk_kline_macd'] == 'decs') or (self.dapan_yes1_beili == 'top')
                    #     dp_curr_cond_good = (self.curr_dp_info['sh']['curr_macd'] > 0 and self.curr_dp_info['sh']['curr_dif'] > 0) and (self.curr_dp_info['sh']['mk_kline_macd'] == 'incs') and (self.dapan_yes1_beili != 'top')
                    #
                    #
                    #     bk_ids_res = self.bk_ids.find({'date': self.today})
                    #     for br in bk_ids_res:
                    #         if br['bk_code'] not in self.curr_bk_info.keys():
                    #             continue
                    #         bk_curr_cond = self.curr_bk_info[br['bk_code']]['mk_green_red_value'] < -4.18 and self.curr_bk_info[br['bk_code']]['green_red'] < 0.28 and self.curr_bk_info[br['bk_code']]['mk_chg_pct'] > 4.18
                    #
                    #         if dp_curr_cond_bad and (not (bk_curr_cond)):
                    #             idsNums_allowed_buy = int(br['idsNums_allowed_first'] * 0.5)
                    #             if br['idsNums_allowed_first'] * 0.5 < 1:
                    #                 idsNums_allowed_buy = 1
                    #             if idsNums_allowed_buy != br['idsNums_allowed_buy']:
                    #                 print(f"{br['bk_code']}chg idsNums_allowed_buy {br['idsNums_allowed_buy']} to {idsNums_allowed_buy}")
                    #                 self.bk_ids.update_one({'date': self.today, 'bk_code': br['bk_code']}, {'$set': {'idsNums_allowed_buy': idsNums_allowed_buy}})
                    #
                    #         elif dp_curr_cond_good and bk_curr_cond:
                    #             idsNums_allowed_buy = int(br['idsNums_allowed_first'] * 2)
                    #             if idsNums_allowed_buy != br['idsNums_allowed_buy']:
                    #                 print(f"{br['bk_code']}chg idsNums_allowed_buy {br['idsNums_allowed_buy']} to {idsNums_allowed_buy}")
                    #                 self.bk_ids.update_one({'date': self.today, 'bk_code': br['bk_code']}, {'$set': {'idsNums_allowed_buy': idsNums_allowed_buy}})
                    #
                    # except Exception as e:
                    #     print(e)

                    tasks_buy = []
                    for code in self.all_jk_buy_list:
                        try:
                            if (not self.curr_bk_info) or (not self.curr_dp_info):
                                continue
                            tasks_buy.append(asyncio.ensure_future(buy_obj[code].jk_buy(self.lastreq[code], curr_time, curr_bk_info, curr_dp_info)))
                        except Exception as e:
                            continue
                            self.logger.error(f"{code}创建jk_buy任务失败，{e}")
                            # if code not in self.lastreq.keys():
                            #     self.lastreq.pop(code)
                            #     self.logger.error(f"{code}创建jk_buy任务失败，{e}, 移除")

                                # print('time??333', time.time()-t)
                    if len(tasks_buy) != 0:
                        loop.run_until_complete(asyncio.wait(tasks_buy))
                    id += 1

                # if curr_time.minute % 25 == 0 and curr_time.second % 59 == 0:
                # print('get_basic_data is alive:', curr_time.time(), len(self.ser))  # pass

            elif curr_time.hour <= 8 or curr_time.hour >= 15 and curr_time.minute >= 2:
                print('Info: not in trading time, break loop', curr_time)
                break

            # print('total time:', time.time() - t)

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

    # 获取回测基础成交数据
    def get_basic_backtest_data(self):
        # 获取监控标的当日的交易数据，并整合
        back_data_df = pd.DataFrame()
        # print(self.all_jk_list)
        wrong_codes = []
        for code in self.all_jk_list:
            file_path = f'./back_test/new_trend_data2/{self.today}/{code}.xlsx'
            if os.path.exists(file_path):
                try:
                    temp_df = pd.read_excel(file_path)
                    # temp_df['code'] = code
                    # print(temp_df)
                    back_data_df['time'] = temp_df['time']
                    back_data_df[code] = temp_df['curr_price']  # back_data_df = pd.merge(back_data_df, temp_df, how='inner')  # back_data_df = back_data_df.append(temp_df, ignore_index=True)

                    # 计算分时macd数据
                    dif, dea, dw = talib.MACD(back_data_df[code]['curr_price'], 720, 1560, 540)
                    macd = (dif - dea) * 2
                    back_data_df[code]['dif'] = dif
                    back_data_df[code]['dea'] = dea
                    back_data_df[code]['macd'] = macd

                    print(back_data_df[code])
                    exit()
                except Exception as e:
                    self.logger.error(e)
                    wrong_codes.append(code)
        if len(wrong_codes) > 0:
            for code in wrong_codes:
                if code in self.all_jk_list:
                    self.all_jk_list.remove(code)

                if code in self.all_jk_buy_list:
                    self.all_jk_buy_list.remove(code)
                if code in self.all_jk_buy_list:
                    self.all_jk_buy_list.remove(code)

        all_jk_low_lst = {code: 999 for code in self.all_jk_list}
        all_jk_high_lst = {code: -1 for code in self.all_jk_list}

        # 创建单个标的对象
        buy_obj = {}
        for code in self.all_jk_list:
            try:
                buy_obj[code] = trend_one(code, self)
            except Exception as e:
                raise e
                self.logger.error(e)

        for index in range(0, 19801):
            t = time.time()
            try:
                # print('len*******************', len(back_data_df))
                per = back_data_df.iloc[index]
                if int(per['time']) > 113000 and int(per['time']) < 130000:
                    continue
                curr_time = self.today + str(int(per['time']))
                del per['time']
                self.ser = per
                self.lastreq = {code: {} for code in self.all_jk_list}
                self.lastreq['curr_time'] = datetime.datetime.strptime(curr_time.zfill(6), '%Y-%m-%d%H%M%S')

                for k, p in self.ser.items():
                    # 记录最大值和最小值
                    if all_jk_low_lst[k] > p:
                        all_jk_low_lst[k] = p

                    if all_jk_high_lst[k] < p:
                        all_jk_high_lst[k] = p

                    # 头几秒可能是空值nan
                    if pd.isna(p):
                        p = 0

                    self.lastreq[k]['curr_price'] = p
                    self.lastreq[k]['today_highest'] = all_jk_high_lst[k]
                    self.lastreq[k]['today_lowest'] = all_jk_low_lst[k]

                sta = time.time()

                tasks = []
                # 创建事件循环对象
                loop = asyncio.get_event_loop()

                for code in self.all_jk_buy_list:
                    try:
                        tasks.append(asyncio.ensure_future(buy_obj[code].jk_buy(self.lastreq[code], self.lastreq['curr_time'])))
                    except Exception as e:
                        raise e
                        self.logger.error(e)

                for code in self.all_jk_sale_list:
                    try:
                        tasks.append(asyncio.ensure_future(buy_obj[code].jk_sale(self.lastreq[code], self.lastreq['curr_time'])))
                    except Exception as e:
                        self.logger.error(e)

                t = time.time()
                if len(tasks) != 0:
                    loop.run_until_complete(asyncio.wait(tasks))  # print(curr_time, '  jk_buy_end_time', time.time() - sta)

            except Exception as e:
                self.logger.error(e)
            print('time:', round(time.time() - t, 2), self.lastreq['curr_time'])
        # 更新当日trend_rec情况
        self.update_trend_rec()

        # 更新当日 整体盈利 情况
        self.update_trend_noSale_money()

    def basic_data2mongo(self, id, curr_time, ser, lastreq):
        data = {}
        data['id'] = id
        data['date'] = self.today
        data['curr_time'] = str(curr_time)
        data['ser'] = str(ser)
        data['lastreq'] = str(lastreq)
        self.basic_data_store.insert_one(data)

    # 获取板块数据情况
    def get_bk_data(self, curr_time, bk_info, gn_info):

        # 获取行业板块
        # url = "https://54.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112305894667514355395_1667748273086&pn=1&pz=500&po=1&np=1&fields=f12%2Cf13%2Cf14%2Cf62&fid=f62&fs=m%3A90%2Bt%3A2&ut=b2884a393a59ad64002292a3e90d46a5&_=1667748273093"

        # f14, f12, f3, f2, f18, f17, f15, f16, f8, f104, f105

        # f14 name, f12 bk_code, f3 chg_pct, f8 turnover_rate, f104 red_code
        # f105 green_code, f2 curr_price, f18 yes_close, f17 open, f15 high, f16 low

        id = 0
        # self.curr_bk_info = {
        #     'bk_name':'',
        #     'rank':-100,
        #     'bk_code':'',
        #     'chg_pct':-100,
        #     'turnover_rate':-100,
        #     'red_code':-100,
        #     'green_code':-100,
        #     'green_red':-100,
        #     'curr_price':-100,
        #     'yes_close':-100,
        #     'open':-100,
        #     'high':-100,
        #     'low':-100,
        #     'type':'',
        #     'num':-100,
        #     'time':'',
        #     'curr_kDif':-100,
        #     'curr_kDea':-100,
        #     'curr_kDw':-100,
        #     'curr_kMacd':-100,
        #     'curr_kMacd_trend':'',
        #     'curr_kMacd_trend_value':-100,
        #     'min_dif':-100,
        #     'min_dea':-100,
        #     'min_dw':-100,
        #     'min_macd':-100,
        #     'price_trend':'',
        #     'min_macd_trend':'',
        #     'mk_chg_pct':-100,
        #     'mk_green_red':-100,
        #     'mk_green_red_value':-100
        # }
        try:
            temp_curr_bk_info = {}
            # 获取板块的时时数据信息
            res = bk_info
            res = bk_info[bk_info.index('[') + 1:bk_info.index(']')]
            res = res.split('},')
            for index, r in enumerate(res):
                try:
                    ndct = {}
                    if index < len(res) - 1:
                        r = r + '}'
                    dct = json.loads(r)

                    if curr_time.hour == 9 and curr_time.minute < 27 and '-' in [dct['f3'], dct['f2'], dct['f18'], dct['f17'], dct['f15'], dct['f16']]:
                        continue

                    if dct['f12'] not in self.bk_jk_lst:
                        continue

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
                    ndct['curr_kMacd_trend'] = self.mk(curr_kMacd[-3:])[0]
                    ndct['curr_kMacd_trend_value'] = self.mk(curr_kMacd[-3:])[1]

                    # 计算最低价时macd
                    # bk_data_list_low = np.append(bk_close_lst, ndct['low'])
                    # low_kline_dif, low_kline_dea, low_kline_dw = talib.MACD(bk_data_list_low, 3, 8, 5)
                    # ndct['low_kline_dif'], ndct['low_kline_dea'], ndct['low_kline_dw'] = low_kline_dif[-1], low_kline_dea[-1], low_kline_dw[-1]
                    # low_kline_macd = (low_kline_dif - low_kline_dea) * 2
                    # ndct['low_kline_macd'] = low_kline_macd[-1]
                    # ndct['low_kline_macd_trend'] = self.mk(low_kline_macd[-3:])[0]

                    # 计算板块分时macd
                    self.bk_min_price_dct[ndct['bk_code']].append(ndct['curr_price'])
                    min_dif, min_dea, min_dw = talib.MACD(np.array(self.bk_min_price_dct[ndct['bk_code']]), 28, 68, 18)
                    ndct['min_dif'], ndct['min_dea'], ndct['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                    min_macd = (min_dif - min_dea) * 2
                    ndct['min_macd'] = min_macd[-1]

                    # 在开盘还未计算出macd时，使用price_trend
                    ndct['price_trend'] = ''
                    # if curr_time.hour == 9 and curr_time.minute <= 45:
                    if np.isnan(ndct['min_macd']) and len(self.bk_min_price_dct[ndct['bk_code']]) > 28:
                        ndct['price_trend'] = self.mk(self.bk_min_price_dct[ndct['bk_code']])[0]

                    if len(min_macd[-28:]) <= 1:
                        ndct['min_macd_trend'] = None
                    else:
                        ndct['min_macd_trend'] = self.mk(min_macd[-28:])[0]

                    if len(self.bk_min_price_dct[ndct['bk_code']]) > 388:
                        self.bk_min_price_dct[ndct['bk_code']].pop(0)

                    # 计算板块的涨跌幅变化趋势
                    self.bk_chg_pct_dct[ndct['bk_code']].append(ndct['chg_pct'])
                    if len(self.bk_chg_pct_dct[ndct['bk_code']]) <= 28:
                        ndct['mk_chg_pct'] = -10
                    else:
                        ndct['mk_chg_pct'] = self.mk(self.bk_chg_pct_dct[ndct['bk_code']])[1]

                    if len(self.bk_chg_pct_dct[ndct['bk_code']]) > 88:
                        self.bk_chg_pct_dct[ndct['bk_code']].pop(0)

                    # 计算板块整体红盘的变化趋势
                    self.bk_chg_green_dct[ndct['bk_code']].append(ndct['green_red'])
                    if len(self.bk_chg_green_dct[ndct['bk_code']]) <= 28:
                        ndct['mk_green_red'] = 'incs'
                        ndct['mk_green_red_value'] = -10
                    else:
                        ndct['mk_green_red'] = self.mk(self.bk_chg_green_dct[ndct['bk_code']])[0]
                        ndct['mk_green_red_value'] = self.mk(self.bk_chg_green_dct[ndct['bk_code']])[1]

                    if len(self.bk_chg_green_dct[ndct['bk_code']]) > 88:
                        self.bk_chg_green_dct[ndct['bk_code']].pop(0)

                    ndct['trd_state'] = False
                    if ndct['curr_kMacd'] > 0 and ndct['curr_kMacd_trend'] == 'incs':
                        ndct['trd_state'] = True

                    temp_curr_bk_info[ndct['bk_code']] = ndct

                    if curr_time.second % 29 == 0:
                        self.fh_bk.write(json.dumps(ndct, ensure_ascii=False) + '\n')
                except Exception as e:
                    # raise e
                    self.logger.error(e)

            res = gn_info
            res = gn_info[gn_info.index('[') + 1:gn_info.index(']')]
            res = res.split('},')
            for index, r in enumerate(res):
                try:
                    ndct = {}
                    if index < len(res) - 1:
                        r = r + '}'
                    dct = json.loads(r)

                    if curr_time.hour == 9 and curr_time.minute < 27 and '-' in [dct['f3'], dct['f2'], dct['f18'], dct['f17'], dct['f15'], dct['f16']]:
                        continue

                    if dct['f12'] not in self.bk_jk_lst:
                        continue

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
                    ndct['curr_kMacd_trend'] = self.mk(curr_kMacd[-3:])[0]
                    ndct['curr_kMacd_trend_value'] = self.mk(curr_kMacd[-3:])[1]

                    # 计算最低价时macd
                    # bk_data_list_low = np.append(bk_close_lst, ndct['low'])
                    # low_kline_dif, low_kline_dea, low_kline_dw = talib.MACD(bk_data_list_low, 3, 8, 5)
                    # ndct['low_kline_dif'], ndct['low_kline_dea'], ndct['low_kline_dw'] = low_kline_dif[-1], low_kline_dea[-1], low_kline_dw[-1]
                    # low_kline_macd = (low_kline_dif - low_kline_dea) * 2
                    # ndct['low_kline_macd'] = low_kline_macd[-1]
                    # ndct['low_kline_macd_trend'] = self.mk(low_kline_macd[-3:])[0]

                    # 计算板块分时macd
                    self.bk_min_price_dct[ndct['bk_code']].append(ndct['curr_price'])
                    min_dif, min_dea, min_dw = talib.MACD(np.array(self.bk_min_price_dct[ndct['bk_code']]), 28, 68, 18)
                    ndct['min_dif'], ndct['min_dea'], ndct['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                    min_macd = (min_dif - min_dea) * 2
                    ndct['min_macd'] = min_macd[-1]

                    # 在开盘还未计算出macd时，使用price_trend
                    ndct['price_trend'] = ''
                    # if curr_time.hour == 9 and curr_time.minute <= 45:
                    if np.isnan(ndct['min_macd']) and len(self.bk_min_price_dct[ndct['bk_code']]) > 28:
                        ndct['price_trend'] = self.mk(self.bk_min_price_dct[ndct['bk_code']])[0]

                    if len(min_macd[-28:]) <= 1:
                        ndct['min_macd_trend'] = None
                    else:
                        ndct['min_macd_trend'] = self.mk(min_macd[-28:])[0]
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
                        ndct['mk_chg_pct'] = self.mk(self.bk_chg_pct_dct[ndct['bk_code']])[1]

                    # 计算板块整体红盘的变化趋势
                    self.bk_chg_green_dct[ndct['bk_code']].append(ndct['green_red'])
                    if len(self.bk_chg_green_dct[ndct['bk_code']]) <= 28:
                        ndct['mk_green_red'] = 'incs'
                        ndct['mk_green_red_value'] = -10
                    else:
                        ndct['mk_green_red'] = self.mk(self.bk_chg_green_dct[ndct['bk_code']])[0]
                        ndct['mk_green_red_value'] = self.mk(self.bk_chg_green_dct[ndct['bk_code']])[1]

                    if len(self.bk_chg_green_dct[ndct['bk_code']]) > 88:
                        self.bk_chg_green_dct[ndct['bk_code']].pop(0)

                    ndct['trd_state'] = False
                    if ndct['curr_kMacd'] > 0 and ndct['curr_kMacd_trend'] == 'incs':
                        ndct['trd_state'] = True

                    temp_curr_bk_info[ndct['bk_code']] = ndct

                    if curr_time.second % 29 == 0:
                        self.fh_bk.write(json.dumps(ndct, ensure_ascii=False) + '\n')
                except Exception as e:
                    # raise e
                    self.logger.error(e)

            return temp_curr_bk_info
        except Exception as e:
            # raise e
            self.logger.error(e)

        id += 1

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
                temp_curr_dp_info[dp_code]['mk_kline_macd'] = self.mk(self.sh_macd_lst[-3:])[0]

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
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = self.mk(min_macd[-28:])[0]

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
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = self.mk(self.dp_chg_pct_dct['sh'])[1]

            elif info[2] == '399001':
                dp_code = 'sz'
                self.sz_price_lst[-1] = float(info[3])

                dif, dea, dw = talib.MACD(self.sz_price_lst, 3, 8, 5)
                macd = (dif - dea) * 2
                self.sz_macd_lst[-1] = macd[-1]
                temp_curr_dp_info[dp_code]['mk_kline_macd'] = self.mk(self.sz_macd_lst[-3:])[0]

                # 计算大盘分时 macd
                self.dp_min_price_dct['sz'].append(float(info[3]))

                min_dif, min_dea, min_dw = talib.MACD(np.array(self.dp_min_price_dct['sz']), 28, 68, 18)
                temp_curr_dp_info[dp_code]['min_dif'], temp_curr_dp_info[dp_code]['min_dea'], temp_curr_dp_info[dp_code]['min_dw'] = min_dif[-1], min_dea[-1], min_dw[-1]
                min_macd = (min_dif - min_dea) * 2
                temp_curr_dp_info[dp_code]['min_macd'] = min_macd[-1]
                if len(min_macd[-28:]) <= 1:
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = None
                else:
                    temp_curr_dp_info[dp_code]['min_macd_trend'] = self.mk(min_macd[-28:])[0]

                if len(self.dp_min_price_dct['sz']) > 388:
                    self.dp_min_price_dct['sz'].pop(0)

                if len(self.dp_chg_pct_dct['sz']) > 38:
                    self.dp_chg_pct_dct['sz'].pop(0)

                if len(self.dp_chg_pct_dct['sz']) < 28:
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = -10
                else:
                    temp_curr_dp_info[dp_code]['mk_pct_chg'] = self.mk(self.dp_chg_pct_dct['sz'])[1]

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
    def get_dp_bk_data(self):

        # 获取行业板块
        # url = "https://54.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112305894667514355395_1667748273086&pn=1&pz=500&po=1&np=1&fields=f12%2Cf13%2Cf14%2Cf62&fid=f62&fs=m%3A90%2Bt%3A2&ut=b2884a393a59ad64002292a3e90d46a5&_=1667748273093"

        # f14, f12, f3, f2, f18, f17, f15, f16, f8, f104, f105

        # f14 name, f12 bk_code, f3 chg_pct, f8 turnover_rate, f104 red_code
        # f105 green_code, f2 curr_price, f18 yes_close, f17 open, f15 high, f16 low

        id = 0
        t = time.time()
        while True:
            # 每秒获取逐笔交易数据
            curr_time = datetime.datetime.now()
            time_cond = (curr_time.hour >= 9 and curr_time.hour < 15) and ((curr_time.hour == 9 and curr_time.minute >= 27) or curr_time.hour >= 10)

            if time_cond:

                # 获取大盘数据
                url = "https://qt.gtimg.cn/q=sh000001,sz399001&r=926277345"

                try:
                    dp_info = Proxy_url.urlget(url)
                except Exception as e:
                    self.logger.error(str(e) + ' will try again')
                    dp_info = req.get(url)

                with self.dp_bk_lock:
                    self.curr_dp_info = copy.deepcopy(self.get_dp_data(str(curr_time), dp_info.text))

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
                    bk_info = res.text

                    gainian_url = f"http://51.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124003105763407314488_1671663240679&pn=1&pz={gn_url_num}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:90+t:3+f:!50&fields=f14,f12,f3,f2,f18,f17,f15,f16,f8,f104,f105&_=1671663240687"
                    # print(gainian_url)
                    res = Proxy_url.urlget(gainian_url)
                    # res = req.get(gainian_url)
                    res.encoding = 'utf-8'
                    gn_info = res.text

                    with self.dp_bk_lock:
                        self.curr_bk_info = copy.deepcopy(self.get_bk_data(curr_time, bk_info, gn_info))

                except Exception as e:
                    # raise e
                    self.logger.error(e)

                id += 1

    def write_bk_info(self):
        # if os.path.exists(f'./bk_info_{self.today}.txt'):
        #     os.remove(f'./bk_info_{self.today}.txt')
        self.fh_bk = open(f'./bk_info_{self.today}.txt', 'a+')

        while True:
            temp_str = ''
            while not self.queue.empty():
                temp_str = temp_str + json.dumps(self.queue.get(), ensure_ascii=False) + '\n'
            else:
                time.sleep(1)

            if temp_str != '':
                self.fh_bk.write(temp_str)

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
        pass
        # while True:
        #     curr_time = datetime.datetime.now()
        #     if curr_time.minute % 25 == 0 and curr_time.second % 59 == 0:
        #         # print("get_reality_cangwei is alive: ", curr_time)
        #         pass
        #     time.sleep(1)
        #     if curr_time.hour >= 9 and curr_time.hour <= 14:
        #         if (curr_time.hour == 11 and curr_time.minute > 30) or curr_time.hour == 12:
        #             continue
        #         elif curr_time.hour >= 15 and curr_time.minute > 1:
        #             break
        #         elif (curr_time.hour == 9 and curr_time.minute >= 36) or curr_time.hour >= 10:
        #
        #             if curr_time.minute % 18 == 0 and curr_time.second % 39 == 0:
        #                 self.trend_reality.drop()
        #                 self.trader.driver.find_element_by_xpath('//*[@id="main"]/div/div[2]/div[1]/ul/li[1]/a').click()
        #                 buy_btn = self.trader.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="tabBody"]')))
        #                 if buy_btn:
        #                     res = self.trader.driver.find_element_by_xpath('//*[@id="tabBody"]')
        #                     res = res.text.split('\n')
        #                     for r in res:
        #                         per = r.split(' ')
        #                         if len(per) != 12:
        #                             continue
        #                         # print(per)
        #                         # 更新当前实际持仓情况
        #                         self.trend_reality.insert_one({'code': self.chg_code_type(per[0]), 'hNum': per[2], 'uNum': per[3], 'money': per[6], 'cost': per[4], 'price': per[5], 'pct': per[8], 'yingkui': per[7]})
        #
        #                         # 查询系统期望可用持仓情况（可用持仓情况只需要查询trend_rec, 不可用持仓则查看hasbuy）
        #                         mongo_res = self.trend_rec.find({'code': self.chg_code_type(per[0]), 'isSold': 0})
        #                         mongo_num = 0
        #                         if mongo_res:
        #                             for m in mongo_res:
        #                                 mongo_num += m['left_num']
        #
        #                             if int(per[3]) > int(mongo_num) and per[0][0] not in ['5', '1']:
        #                                 # 卖出不一致仓位
        #                                 sold_num = int(per[3]) - int(mongo_num)
        #                                 sale_price = round(float(per[5]) * 0.99, 2)
        #                                 try:
        #                                     sold_num = 'all' if sold_num <= 100 else int(sold_num / 100) * 100
        #                                     with self.trade_lock:
        #                                         res = self.trader.auto_sale_chrome(self.chg_code_type(per[0]), sale_price, sold_num)
        #                                         self.logger.info(f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, curr_price:{float(per[5])}, num:{sold_num}, response:{res}")  # # send_notice('sold_point_get_reality_cangwei_success', f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, curr_price:{float(per[5])}, num:{sold_num}, response:{res}")  # time.sleep(5)
        #                                 except Exception as e:
        #                                     self.logger.error(f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, time:{curr_time}, sold_type:卖出不一致, e:{e}")  # # send_notice('sold_point_get_reality_cangwei_faild', f"sold_point_get_reality_cangwei: {self.chg_code_type(per[0])}, time:{curr_time}, sold_type:卖出不一致, e:{e}")  # time.sleep(5)

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

        elif self.run_type in  ['trading', 'trading_debug']:
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
                        self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price': cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0,
                                                   'highest_price': cost})  # 获取每支个股最近插入的背离数据  # self.trend_rec.insert_one({'code': k, 'price': cost, 'curr_price':cost, 'time': str(curr_time), 'buy_date': self.today, 'bt': 1, 'money': buyMoney, 'isSold': 0, 'cost': cost, 'num': num, 'pct_bt': 0, 'yingkui': 0, 'yingkui1': 0, 'yingkui2': 0, 'yingkui3': 0, 'left_num': num, 'bts': bts, 'st': 0, 'highest_price': cost})  # 获取每支个股最近插入的背离数据

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


def drop_coll(run_type):
    # 获取当前执行文件的名称
    file_name = str(os.path.basename(__file__).split('.')[0])
    # 连接mongoDB
    myclient = pm.MongoClient("mongodb://localhost:27017/")
    fd = myclient["freedom"]

    fd.drop_collection(file_name + 'trend_rec')
    # fd.drop_collection(file_name + '_beili')
    fd.drop_collection(file_name + 'trend_has_buy')
    fd.drop_collection(file_name + 'noSale_money')
    fd.drop_collection(file_name + 'all_trd_jklist_backtest')

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

            # today_int = int(f"{today[0:4]}{today[5:7]}{today[8:]}")
            # if today_int < 20210615:
            #     continue

            jk_buy_lst = list(pd.read_excel(f'./find_trend/backtest_k_daily4/jk_buy_list/{today}.xlsx')['ts_code'])

            print('*' * 88)
            print(today, yestoday, len(jk_buy_lst))
            print('*' * 88)

            trend = Trend(today, yestoday, run_type, jk_buy_lst)

            trend.get_basic_backtest_data()

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

    elif run_type in ['trading', 'trading_debug']:
        today = str(datetime.datetime.now().date())
        # today = "2023-02-10"
        trend = Trend(today, '2023-03-07', run_type)

        thr_dct = {}

        if run_type == 'trading_debug':
            trend.get_basic_data()
            # thr_dct[1] = thr.Thread(target=trend.get_basic_data)
            # thr_dct[2] = thr.Thread(target=trend.get_dp_bk_data)
            # thr_dct[3] = thr.Thread(target=trend.keep_login)
            # thr_dct[4] = thr.Thread(target=trend.get_reality_cangwei)

            # thr_dct[1].start()
            # thr_dct[2].start()
            # thr_dct[3].start()
            # thr_dct[4].start()

        elif run_type == 'trading':
            thr_dct[1] = thr.Thread(target=trend.get_basic_data)
            thr_dct[2] = thr.Thread(target=trend.get_dp_bk_data)
            thr_dct[3] = thr.Thread(target=trend.keep_login)
            thr_dct[4] = thr.Thread(target=trend.get_reality_cangwei)

            thr_dct[1].start()
            thr_dct[2].start()
            thr_dct[3].start()
            thr_dct[4].start()

if __name__ == '__main__':
    # 配置runtype: backtest|trading| trading_debug
    run_type = 'trading'
    main(run_type)
