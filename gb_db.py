# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
import pandas as pd
import tqdm
# import janitor
import time

import pymysql
import pymysql.cursors
import mysql.connector as sql
from sqlalchemy import create_engine
import requests
from io import BytesIO
import sys
from pandas.tseries.offsets import BDay
# from core.gb_logger import gb_log
# from kwcommon.kwcomA1con import DB_ADDRESS, DB_TRADING_DATA_BASE_NAME, DB_PORT, DB_ID, DB_PWD, DB_DAILY_DATA_BASE_NAME, DB_MINUTE_DATA_BASE_NAME, HC_DB_ADDRESS, HC_DB_PORT, HC_DB_ID, HC_DB_PWD, DB_ACCOUNT_DATA_BASE_NAME, DB_TELEGRAM_DATA_BASE_NAME, DB_INVESTOR_DATA_BASE_NAME, GB_TRADER_ROOT_PATH
from util_ds import is_KRX_on_day
from functools import lru_cache

load_dotenv()

# GB_TRADER_ROOT_PATH = '' # TODO web deploy 시는 어떻게 하나... 
GB_TRADER_ROOT_PATH = r'C:\Users\DSK\GB_TRADER_ROOT'

str_recent_BDay = (pd.datetime.today() + BDay(1)-BDay(1)).strftime('%Y%m%d')  # TODO : KRX calendar 추후 반영

def is_business_day(date):
    return bool(len(pd.bdate_range(date, date)))

from pykrx import stock


class Gb_Db():

    DB_TRADING_DATA_BASE_NAME = 'KWDB_TRADING'
    DB_DAILY_DATA_BASE_NAME = 'KWDB_DAILY'
    DB_MINUTE_DATA_BASE_NAME = 'KWDB_MINUTE'
    DB_INVESTOR_DATA_BASE_NAME = 'KWDB_INVESTOR'
    DB_TELEGRAM_DATA_BASE_NAME = 'KWDB_TELEGRAM'
    DB_ACCOUNT_DATA_BASE_NAME = 'KWDB_ACCOUNT_DSK'

    DB_ID = os.getenv('AWS_DB_ID')
    DB_PWD = os.getenv('AWS_DB_PWD')
    DB_ADDRESS = os.getenv('AWS_DB_ADDRESS')
    DB_PORT = os.getenv('AWS_DB_PORT')

    # db_name = os.getenv('db_name')

    def __init__(self, source='AWS'):

        if source == 'HC':
            self.DB_ID = os.getenv('HC_DB_ID') 
            self.DB_PWD = os.getenv('HC_DB_PWD')
            self.DB_ADDRESS = os.getenv('HC_DB_ADDRESS')
            self.DB_PORT = os.getenv('HC_DB_PORT')

        elif source == 'YE':
            self.DB_ID = os.getenv('YE_DB_ID') 
            self.DB_PWD = os.getenv('YE_DB_PWD')
            self.DB_ADDRESS = os.getenv('YE_DB_ADDRESS')
            self.DB_PORT = os.getenv('YE_DB_PORT')

        ## KWDB_TRADING (AWS)
        self.db_trading_connection = None
        self.db_trading_con = None

        ## KWDB_DAILY (AWS)
        self.db_daily_connection = None
        self.db_daily_con = None

        ## DAILY_NAVER (YE)
        self.db_daily_naver_connection = None
        self.db_daily_naver_con = None

        ## KWDB_MINUTE (AWS)
        self.db_minute_connection = None
        self.db_minute_con = None

        ## KWDB_TELEGRAM (AWS)
        self.db_telegram_connection = None
        self.db_telegram_con = None

        ## KWDB_INVESTOR (AWS)
        self.db_investor_connection = None
        self.db_investor_con = None

        ## KWDB_INVESTOR_PROCESSED (AWS)
        self.db_investor_processed_connection = None
        self.db_investor_processed_con = None

        ## KWDB_TEST (AWS)
        self.db_test_connection = None
        self.db_test_con = None

        ## KWDB_STOCK_MASTER (AWS)
        self.db_stockmaster_connection = None
        self.db_stockmaster_con = None

        ## KWDB_ACCOUNT (LOCAL)
        self.db_account_connection = None
        self.df_stock_master = None

        # self.get_stock_master_data()

        ## Trackdown missing datas
        self.missing_symbols = []
        self.missing_symbols_investor = []

    def get_db_trading_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_trading_connection:
            self.db_trading_connection = sql.connect(host=self.DB_ADDRESS, database=self.DB_TRADING_DATA_BASE_NAME, port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')
        return self.db_trading_connection.cursor(cursor_type)

    def get_db_trading_con(self):
        if None == self.db_trading_con:
            self.db_trading_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, self.DB_TRADING_DATA_BASE_NAME),
                                                encoding='utf8',
                                                echo=False)
            self.db_trading_con.connect()
        return self.db_trading_con

    def get_db_stockmaster_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_stockmaster_connection:
            self.db_stockmaster_connection = sql.connect(host=self.DB_ADDRESS, database='KWDB_STOCK_MASTER', 
                                                    port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')
        return self.db_stockmaster_connection.cursor(cursor_type)

    def get_db_stockmaster_con(self):
        if None == self.db_stockmaster_con:
            self.db_stockmaster_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, 'KWDB_STOCK_MASTER'),
                                                encoding='utf8',
                                                echo=False)
            self.db_stockmaster_con.connect()
        return self.db_stockmaster_con

    def get_db_daily_naver_con(self):
        if None == self.db_daily_naver_con:
            self.db_daily_naver_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, 'daily_naver'),
                                                encoding='utf8',
                                                echo=False)
            self.db_daily_naver_con.connect()
        return self.db_daily_naver_con

    def get_db_account_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_account_connection:
            self.db_account_connection = sql.connect(host=HC_DB_ADDRESS, database=self.DB_ACCOUNT_DATA_BASE_NAME, port=HC_DB_PORT, user=HC_DB_ID, password=HC_DB_PWD, charset='utf8')
        return self.db_account_connection.cursor(cursor_type)

    def get_db_daily_connection(self):
        if None == self.db_daily_connection:
            self.db_daily_connection = sql.connect(host=self.DB_ADDRESS, database=self.DB_DAILY_DATA_BASE_NAME, port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')
        return self.db_daily_connection

    def get_db_daily_con(self):
        if None == self.db_daily_con:
            self.db_daily_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                          .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, self.DB_DAILY_DATA_BASE_NAME),
                                          encoding='utf8',
                                          echo=False)
            self.db_daily_con.connect()
        return self.db_daily_con

    def get_db_minute_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_minute_connection:
            self.db_minute_connection = sql.connect(host=self.DB_ADDRESS, database=self.DB_MINUTE_DATA_BASE_NAME, port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')
        return self.db_minute_connection.cursor(cursor_type)

    def get_db_minute_con(self):
        if None == self.db_minute_con:
            self.db_minute_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, self.DB_MINUTE_DATA_BASE_NAME),
                                                encoding='utf8',
                                                echo=False)
            self.db_minute_con.connect()

        return self.db_minute_con

    def get_db_telegram_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_telegram_connection:
            self.db_telegram_connection = sql.connect(host=self.DB_ADDRESS, database=self.DB_TELEGRAM_DATA_BASE_NAME, port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')

        return self.db_telegram_connection.cursor(cursor_type)

    def get_db_telegram_con(self):
        if None == self.db_telegram_con:
            self.db_telegram_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, self.DB_TELEGRAM_DATA_BASE_NAME),
                                                encoding='utf8',
                                                echo=False)
            self.db_telegram_con.connect()
        return self.db_telegram_con

    def get_db_investor_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_investor_connection:
            self.db_investor_connection = sql.connect(host=self.DB_ADDRESS, database=self.DB_INVESTOR_DATA_BASE_NAME, port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')

        return self.db_investor_connection.cursor(cursor_type)

    def get_db_investor_con(self):
        if None == self.db_investor_con:
            self.db_investor_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, self.DB_INVESTOR_DATA_BASE_NAME),
                                                encoding='utf8',
                                                echo=False)
            self.db_investor_con.connect()
        return self.db_investor_con

    def get_db_investor_processed_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_investor_processed_connection:
            self.db_investor_processed_connection = sql.connect(host=self.DB_ADDRESS, database='KWDB_INVESTOR_PROCESSED', port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')

        return self.db_investor_processed_connection.cursor(cursor_type)

    def get_db_investor_processed_con(self):
        if None == self.db_investor_processed_con:
            self.db_investor_processed_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, 'KWDB_INVESTOR_PROCESSED'),
                                                encoding='utf8',
                                                echo=False)
            self.db_investor_processed_con.connect()
        return self.db_investor_processed_con

    def get_db_test_connection(self, cursor_type = pymysql.cursors.DictCursor):
        if None == self.db_test_connection:
            self.db_test_connection = sql.connect(host=self.DB_ADDRESS, database='KWDB_TEST', port=self.DB_PORT, user=self.DB_ID, password=self.DB_PWD, charset='utf8')

        return self.db_test_connection.cursor(cursor_type)

    def get_db_test_con(self):
        if None == self.db_test_con:
            self.db_test_con = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'
                                                .format(self.DB_ID, self.DB_PWD, self.DB_ADDRESS, self.DB_PORT, 'KWDB_TEST'),
                                                encoding='utf8',
                                                echo=False)
            self.db_test_con.connect()
        return self.db_test_con

    def get_all_tables(self, server = "aws"):
        if server == "aws":
            dbcur = self.get_db_trading_connection()
        else:
            dbcur = self.get_db_account_connection()
        table_names = []
        try:
            dbcur.execute("SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA = '{}'".format(self.DB_TRADING_DATA_BASE_NAME))
            tables = dbcur.fetchall()
            for table in tables:
                table_names.append(table)
        except Exception as e:
            # gb_log.exception(e)
            print(e)
        return table_names

    def get_my_stock_tables_from_db(self, accout_id, isVirtual):
        pre = 'MyStock_{}'.format(accout_id)
        post = '{}'.format('virtual' if isVirtual else 'real')
        my_stock_tables = []
        all_talbes = self.get_all_tables("harry")
        for each in all_talbes:
            table_name = each[2]
            if table_name.startswith(pre) and table_name.endswith(post):
                my_stock_tables.append(table_name)

        return my_stock_tables

    def get_my_stock_names(self, table_name):
        dbcur = self.get_db_trading_connection()
        codes = []
        items = None

        try:
            dbcur.execute("SELECT * FROM {}".format(table_name))
            items = dbcur.fetchall()
        except Exception as e:
            print(e)
        finally:
            dbcur.close()

        if items:
            for item in items:
                codes.append(item[1])
        return codes

    def check_table_exist(self, tablename: str, db_name : str = None):
        if db_name == None:
            db_name = self.DB_TRADING_DATA_BASE_NAME
        result = False
        if db_name == self.DB_TRADING_DATA_BASE_NAME:
            dbcur = self.get_db_trading_connection()
        elif db_name == self.DB_ACCOUNT_DATA_BASE_NAME:
            dbcur = self.get_db_account_connection()
        elif db_name == self.DB_TELEGRAM_DATA_BASE_NAME:
            dbcur = self.get_db_telegram_connection()
        elif db_name == self.DB_DAILY_DATA_BASE_NAME:
            dbcur = self.get_db_telegram_connection()
        elif db_name == self.DB_MINUTE_DATA_BASE_NAME:
            dbcur = self.get_db_telegram_connection()
        elif db_name == self.DB_INVESTOR_DATA_BASE_NAME:
            dbcur = self.get_db_investor_connection()
        else:
            print(f'db_name = {db_name} not implement.')
            dbcur = self.get_db_test_connection()

        str_sql = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = '{0}'
                """.format(tablename.replace('\'', '\'\''))
        try:
            dbcur.execute(str_sql)
            if dbcur.fetchone()[0] >= 1:
                result = True
        except Exception as e:
            print(e)
        finally:
            dbcur.close()
            return result

    def check_item_exist(self, table_name: str, key: str, data: str):
        result = False
        dbcur = self.get_db_trading_connection()
        try:
            sql = f"SELECT COUNT(*) FROM {table_name} WHERE {key} = '{data}';"
            print(f'check_item_exist | {sql}')
            dbcur.execute(sql)
            num_count = dbcur.fetchone()[0]
            print(f'number of count {num_count}')
            if num_count >= 1:
                result = True
        except Exception as e:
            print(e)
        finally:
            dbcur.close()
            return result

    def check_day_stock_exist(self, stock_code: str):
        table = 'opt10081_{}_tbl'.format(stock_code)
        return self.check_table_exist(table)

    def check_investor_info_exist(self):
        table = 'opt10060tbl'

    def check_minute_stock_exist(self, stock_code: str):
        table = 'opt10080_{}_tbl'.format(stock_code)
        return self.check_table_exist(table)

    def get_stock_master_data(self, tablename: str = 'all_stock_info'):
        self.get_db_trading_con()
        df_tmp = pd.read_sql_table(tablename, self.db_trading_con)
        return df_tmp

    def get_ebest_theme(self, tablename: str = 'ebest_theme'):
        self.get_db_trading_con()
        df_tmp = pd.read_sql_table(tablename, self.db_trading_con)
        return df_tmp

    def get_ebest_theme_most_recent(self, tablename: str = 'ebest_theme'):
        df_tmp = self.get_ebest_theme()
        df_tmp = df_tmp.filter_date('일자', df_tmp['일자'].max()) 
        return df_tmp

    def get_stock_name(self, code: str):
        try:
            df_result = self.df_stock_master.loc[self.df_stock_master['종목코드'] == code]
            return df_result['종목명'].values[0]
        except Exception as e:
            print(e)
            return ""

    def get_stock_code(self, name: str):
        try:
            df_result = self.df_stock_master.loc[self.df_stock_master['종목명'] == name]
            val = df_result['종목코드']
            return df_result['종목코드'].values[0]
        except Exception as e:
            # print('{} is not in df_stock_master' . format(name))
            return ""

    def get_day_data(self, stock_code: str):
        table = 'opt10081_{}_tbl'.format(stock_code)
        # count = 0
        # strSql = 'SELECT COUNT(*) FROM {}'.format(table)
        # cursor_daily = self.db_daily_connection.cursor()
        # try:
        #     cursor_daily.execute(strSql)
        #     count = int(cursor_daily.fetchone()[0])
        # except Exception as e:
        #     print(e)
        # finally:
        #     cursor_daily.close()
        con = self.get_db_daily_con()
        strSql = 'SELECT * FROM {} ORDER BY 일자 DESC;'.format(table)
        df_daily = pd.read_sql(strSql, con)

        return df_daily

    def get_minute_data(self, stock_code: str):
        con = self.get_db_minute_con()
        table = 'opt10080_{}_tbl'.format(stock_code)
        # count = 0
        # strSql = 'SELECT COUNT(*) FROM {}'.format(table)
        # cursor_minute = self.db_minute_connection.cursor()
        # try:
        #     cursor_minute.execute(strSql)
        #     count = int(cursor_minute.fetchone()[0])
        # except Exception as e:
        #     print(e)
        # finally:
        #     cursor_minute.close()

        strSql = 'SELECT * FROM {} ORDER BY 체결시간 DESC;'.format(table)
        df_daily = pd.read_sql(strSql, con)

        return df_daily

    def get_specific_day_data(self, stock_code: str, cache_local_folder=False ,start_date='21001231', end_date='19790719', order='asc', from_='AWS'):
        '''
        param : 
            start_date : YYYYMMDD, end_date : YYYYMMDD
        '''
        if from_ == 'AWS':
            con = self.get_db_daily_con()

            if cache_local_folder: # local folder 있으면 먼저 찾아보고, 없으면 거기 저장
                file_name = f'DAY_{stock_code}_DATE({start_date}_{end_date}).csv'
                csv_path = os.path.join(GB_TRADER_ROOT_PATH, 'csv_day', file_name)
                if os.path.exists(csv_path):
                    df_daily = pd.read_csv(csv_path, parse_dates=['일자'])
                else:
                    strSql = f'SELECT 일자, 현재가, 시가, 고가, 저가, 거래량 FROM opt10081_{stock_code}_tbl WHERE 일자 BETWEEN "{start_date}" AND "{end_date}" ORDER BY 일자 {order}'
                    df_daily = pd.read_sql(strSql, con)
                    df_daily.to_csv(csv_path, index=False)
            else: # local folder 없는겨우 바로 반환
                strSql = f'SELECT 일자, 현재가, 시가, 고가, 저가, 거래량 FROM opt10081_{stock_code}_tbl WHERE 일자 BETWEEN "{start_date}" AND "{end_date}" ORDER BY 일자 {order}'
                df_daily = pd.read_sql(strSql, con)
            return df_daily
        elif from_ == 'pykrx':
            df_daily = (
                stock.get_market_ohlcv_by_date(start_date, end_date, stock_code)
                .reset_index()
                .rename(columns={'날짜':'일자','종가': '현재가'}))
            return df_daily
        else:
            print('specify data source')

    def get_specific_day_data_multi(self, symbols, cache_local_folder=False, **kwargs):
        data  = pd.DataFrame()
        for code in tqdm.tqdm(symbols):
            try:
                df = self.get_specific_day_data(code, cache_local_folder, **kwargs)
            except Exception as e:
                print(e)
                print(f'missing db table : {code}')
                self.missing_symbols.append(code)
                continue
            if len(df) == 0:
                print('missing',code)
            df['code'] = code
            if data.empty:
                data = df
            else:
                data = pd.concat([data, df])
        return data                

    def get_specific_day_data_multi_pykrx_AWS(self, symbols, cache_local_folder=False, **kwargs):
            data  = pd.DataFrame()
            for code in tqdm.tqdm(symbols):
                try:
                    df = self.get_specific_day_data(code, cache_local_folder, **kwargs, from_='pykrx')
                    time.sleep(0.5)
                    if len(df) == 0 or df is None:
                        print('missing',code)
                        print('len == 0 while read from pykrx')
                        print('try from AWS')
                        try:
                            df = self.get_specific_day_data(code, cache_local_folder, **kwargs, from_='AWS')
                        except Exception as e2:
                            print(e2)
                            print(f'missing db table : {code}')
                            raise
                except Exception as e:
                    print(e)
                    self.missing_symbols.append(code)
                    try:
                        print('try from AWS')
                        df = self.get_specific_day_data(code, cache_local_folder, **kwargs, from_='AWS')
                    except Exception as e2:
                        print(e2)
                        print(f'missing db table : {code}')
                        raise
                df['code'] = code
                if data.empty:
                    data = df
                else:
                    data = pd.concat([data, df])
            return data                


    def get_specific_investor_data(self, stock_code: str, **kwargs):
        con = self.get_db_investor_con()
        end_date = kwargs['end_date'] if 'end_date' in kwargs else "2100-12-31"
        start_date = kwargs['start_date'] if 'start_date' in kwargs else "1979-07-19"
        order = kwargs['order'] if 'order' in kwargs else "asc"

        strSql = f'SELECT 일자, 종목코드, 현재가, 누적거래대금, 개인투자자, 외국인투자자, 기관계, 기타법인, 내외국인, 금융투자, 연기금등, 사모펀드, 투신, 국가, 기타금융, 은행, 보험 FROM opt10060_{stock_code}_tbl WHERE 종목코드 = "{stock_code}" AND 일자 BETWEEN "{start_date}" AND "{end_date}" ORDER BY 일자 {order}'

        df_daily = pd.read_sql(strSql, con)
        return df_daily

    def get_specific_investor_data_multi(self, symbols, **kwargs):
        data = pd.DataFrame()
        for code in tqdm.tqdm(symbols):
            try:
                df = self.get_specific_investor_data(code, **kwargs)
            except Exception as e:
                print(e)
                print(f'missing investor db table : {code}')
                self.missing_symbols_investor.append(code)
                continue
            if len(df) == 0:
                print('missing',code)
            df['code'] = code
            if data.empty:
                data = df
            else:
                data = pd.concat([data, df])
        return data

    # def get_specific_minute_data(self, stock_code: str, start_date, end_date = "", order="DESC"):
    #     con = self.get_db_minute_con()
    #     if end_date.lower() == 'desc' or end_date.lower() == 'asc':
    #         order = end_date
    #         end_date = ""
    #
    #     if len(end_date):
    #         strSql = 'SELECT 체결시간, 현재가, 시가, 고가, 저가, 거래량 FROM opt10080_{}_tbl WHERE 체결시간 BETWEEN "{}" AND "{}" ORDER BY 체결시간 {}'.format(stock_code, start_date, end_date, order)
    #     else:
    #         strSql = 'SELECT 체결시간, 현재가, 시가, 고가, 저가, 거래량 FROM opt10080_{}_tbl WHERE 체결시간 >= "{}" ORDER BY 체결시간 {}'.format(stock_code, start_date, order)
    #     df_minute = pd.read_sql(strSql, con)
    #     return df_minute

    def get_specific_minute_data(self, stock_code: str, **kwargs):
        con = self.get_db_minute_con()
        end_date = kwargs['end_date'] if 'end_date' in kwargs else "2100-12-31"
        start_date = kwargs['start_date'] if 'start_date' in kwargs else "1979-07-19"
        order = kwargs['order'] if 'order' in kwargs else "asc"
        start_time = kwargs['start_time'] if 'start_time' in kwargs else "09:00"
        end_time = kwargs['end_time'] if 'end_time' in kwargs else "15:20"
        verify = kwargs['verify'] if 'verify' in kwargs else False
        try:
            if end_date.lower() == 'desc' or end_date.lower() == 'asc':
                order = end_date
                end_date = ""
        except:
            pass
            
        csv_name = f'M_{stock_code}_DATE({start_date}_{end_date}).csv'.replace(':','_')
        csv_path = os.path.join(GB_TRADER_ROOT_PATH, 'csv_min', csv_name)
        from_server = False
        try:
            if not verify and os.path.exists(csv_path):
                # df_minute = pd.read_csv(csv_path, index_col=0, parse_dates=['체결시간'])
                df_minute = pd.read_csv(csv_path, parse_dates=['체결시간'])
            else:
                from_server = True
        except Exception as e:
            from_server = True

        if from_server:
            strSql = f'SELECT 체결시간, 현재가, 시가, 고가, 저가, 거래량 FROM opt10080_{stock_code}_tbl WHERE 체결시간 BETWEEN "{start_date}" AND "{end_date}" AND DATE_FORMAT(체결시간, "%%H:%%i") >= "{start_time}" AND DATE_FORMAT(체결시간, "%%H:%%i") < "{end_time}" ORDER BY 체결시간 {order}'
            print(strSql)
            df_minute = pd.read_sql(strSql, con)
            df_minute.to_csv(csv_path, index=False)

        try:
            if verify:
                tmp = df_minute['체결시간'][0]
                start_db = tmp.strftime('%Y-%m-%d %H:%M')
                start_input = f'{start_date} {start_time}'
                if start_db[:-2] != start_input[:-2]:
                    print(f'verify result | {stock_code} data error | DB_Date({start_db}) != Input_Date({start_input})')
                    self.delete_minute_data(stock_code)
                    os.remove(csv_path)
                    return pd.DataFrame()
        except Exception as e:
            print(e)
            print(f'remove {stock_code} table because, invalid data catched.')
            self.delete_minute_data(stock_code)
            os.remove(csv_path)
            return pd.DataFrame()
        return df_minute

    def get_specific_kospi_data(self, start_date, end_date = "", order="DESC"):
        con = self.get_db_daily_con()
        if end_date.lower() == 'desc' or end_date.lower() == 'asc':
            order = end_date
            end_date = ""

        if len(end_date):
            strSql = 'SELECT 일자, 현재가, 시가, 고가, 저가, 거래량, 거래대금 FROM opt20006_001_tbl WHERE 일자 BETWEEN "{}" AND "{}" ORDER BY 일자 {}'.format(start_date, end_date, order)
        else:
            strSql = 'SELECT 일자, 현재가, 시가, 고가, 저가, 거래량, 거래대금 FROM opt20006_001_tbl WHERE 일자 >= "{}" ORDER BY 일자 {}'.format(start_date, order)
        df_kospi = pd.read_sql(strSql, con)
        return df_kospi

    def get_specific_kosdaq_data(self, start_date, end_date = "", order="DESC"):
        con = self.get_db_daily_con()
        if end_date.lower() == 'desc' or end_date.lower() == 'asc':
            order = end_date
            end_date = ""

        if len(end_date):
            strSql = 'SELECT 일자, 현재가, 시가, 고가, 저가, 거래량, 거래대금 FROM opt20006_101_tbl WHERE 일자 BETWEEN "{}" AND "{}" ORDER BY 일자 {}'.format(start_date, end_date, order)
        else:
            strSql = 'SELECT 일자, 현재가, 시가, 고가, 저가, 거래량, 거래대금 FROM opt20006_101_tbl WHERE 일자 >= "{}" ORDER BY 일자 {}'.format(start_date, order)
        df_kospi = pd.read_sql(strSql, con)
        return df_kospi

    def delete_minute_data(self, stock_code: str):
        dbcur = self.get_db_minute_connection()
        strSql = 'DROP TABLE opt10080_{}_tbl'.format(stock_code)
        result = False
        try:
            dbcur.execute(strSql)
            result = True
        except Exception as e:
            print(e)
        finally:
            dbcur.close()
            return result

    def check_minute_table_exist(self, stock_code: str):
        dbcur = self.get_db_minute_connection()
        result = False
        tablename = 'opt10080_{}_tbl'.format(stock_code)
        try:
            dbcur.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = '{0}'
                """.format(tablename.replace('\'', '\'\'')))
            if dbcur.fetchone()[0] >= 1:
                result = True
        except Exception as e:
            print(e)
        finally:
            dbcur.close()
            return result

    def update_index_table_daily(self, stock_code: str):
        dbcur = self.get_db_daily_connection()
        tablename = 'opt10081_{}_tbl'.format(stock_code)
        stt_sql = f"ALTER TABLE {tablename} ADD INDEX {'index_date'} ( {'일자'} ); "

        try:
            dbcur.execute(stt_sql)
            result = True
            print(f'success create index table {stock_code}')
        except Exception as e:
            print(e)
            result = False
        finally:
            dbcur.close()
            return result

    def update_index_table_minute(self, stock_code: str):
        dbcur = self.get_db_minute_connection()
        tablename = 'opt10080_{}_tbl'.format(stock_code)
        stt_sql = f"ALTER TABLE {tablename} ADD INDEX {'index_date'} ( {'체결시간'} ); "

        result = False
        try:
            dbcur.execute(stt_sql)
            result = True
            print(f'success create index table {stock_code}')
        except Exception as e:
            print(e)
        finally:
            dbcur.close()
            return result

    def get_stock_info(self, stock_name: str):
        con = self.get_db_trading_con()
        strSql = f'SELECT T1.*, T2.* from all_stock_info as T1, 상승률_네이버 as T2 where T1.종목명 = "{stock_name}" and T2.종목명 = "{stock_name}" ORDER BY T2.날짜 DESC LIMIT 1'

        df_stock_info = pd.read_sql(strSql, con)
        return df_stock_info

    def insert_telegram_raw_data(self, msg_index, channel_name, channel_id, date, raw_data):
        dbcur = self.get_db_telegram_connection()
        table_name = "test_channel"
        if channel_id == TELEGRAM_CHANNEL_ID_증시의신:
            table_name = "증시의신"
        elif channel_id == TELEGRAM_CHANNEL_ID_공시정보:
            table_name = "공시정보"
        elif channel_id == TELEGRAM_CHANNEL_ID_주식_공시알리미:
            table_name = "공시알리미"
        elif channel_id == TELEGRAM_CHANNEL_ID_아이투자_텔레그램:
            table_name = "아이투자"
        else:
            table_name = "test_channel"

        str_sql = f'INSERT INTO {table_name} (`Index`, `Channel Name`, `Channel ID`, `Date`, `Raw Data`) VALUES ("{msg_index}", "{channel_name}", "{channel_id}", "{date}", "{raw_data}");'
        try:
            dbcur.execute(str_sql)
            self.db_telegram_connection.commit()
            result = True
            print(f'success insert data {channel_name}, {msg_index}, {date} in {table_name}')
        except Exception as e:
            print(e)
        finally:
            dbcur.close()

    def get_sepcific_telegram_data(self, channel_id, **kwargs): # start_date, end_date="", order="DESC"):
        con = self.get_db_telegram_con()
        table_name = "test_channel"
        if channel_id == TELEGRAM_CHANNEL_ID_증시의신:
            table_name = "증시의신"
        elif channel_id == TELEGRAM_CHANNEL_ID_공시정보:
            table_name = "공시정보"
        elif channel_id == TELEGRAM_CHANNEL_ID_주식_공시알리미:
            table_name = "공시알리미"
        elif channel_id == TELEGRAM_CHANNEL_ID_아이투자_텔레그램:
            table_name = "아이투자"
        else:
            table_name = "test_channel"

        end_date = kwargs['end_date'] if 'end_date' in kwargs else "2100-12-31"
        start_date = kwargs['start_date'] if 'start_date' in kwargs else "2000-01-01"
        order = kwargs['order'] if 'order' in kwargs else "asc"
        specific_time = kwargs['specific_time'] if 'specific_time' in kwargs else ""
        key_word = kwargs['key_word'] if 'key_word' in kwargs else ""
        limit = kwargs['limit'] if 'limit' in kwargs else 0
        str_limit = ""
        if limit > 0:
            str_limit = f" LIMIT {limit}"
        if len(key_word):
            strSql = f'SELECT Date, `Index`, `Raw Data`, `Channel Name` FROM {table_name} WHERE `Raw Data` LIKE "%%{key_word}%%" ORDER BY Date {order}{str_limit}'

            print(strSql)
            df_data = pd.read_sql(strSql, con)
            return df_data

        if len(specific_time):
            strSql = f'SELECT Date, `Index`, `Raw Data`, `Channel Name` FROM {table_name} WHERE Date = "{specific_time}"'
            df_data = pd.read_sql(strSql, con)
            return df_data

        if len(end_date):
            strSql = f'SELECT Date, `Index`, `Raw Data`, `Channel Name` FROM {table_name} WHERE Date BETWEEN "{start_date}" AND "{end_date}" ORDER BY Date {order}'
        else:
            strSql = f'SELECT Date, `Index`, `Raw Data`, `Channel Name` FROM {table_name} WHERE Date >= "{start_date}" ORDER BY Date {order}'
        df_data = pd.read_sql(strSql, con)
        return df_data

    def get_last_telegram_data(self, channel_id):
        con = self.get_db_telegram_con()
        table_name = "test_channel"
        if channel_id == TELEGRAM_CHANNEL_ID_증시의신:
            table_name = "증시의신"
        elif channel_id == TELEGRAM_CHANNEL_ID_공시정보:
            table_name = "공시정보"
        elif channel_id == TELEGRAM_CHANNEL_ID_주식_공시알리미:
            table_name = "공시알리미"
        elif channel_id == TELEGRAM_CHANNEL_ID_아이투자_텔레그램:
            table_name = "아이투자"
        else:
            table_name = "test_channel"

        strSql = f'SELECT Date, `Index`, `Raw Data`, `Channel Name` FROM {table_name} ORDER BY Date DESC LIMIT 1'
        df_data = pd.read_sql(strSql, con)
        return df_data

    def get_종목명_종목코드_AWS(self):
        tablename = '종목명_종목코드_시장'
        try: # 먼저 table 읽어보고
            con = self.db_trading_con
            df_tmp = pd.read_sql_table(tablename, con=con, index_col='index')
            print(len(df_tmp))
            return df_tmp
        except Exception as e:
            print(e)

    @lru_cache(maxsize=10)     
    def get_상장법인정보_kind(self, str_date=str_recent_BDay, to_sql=True):
        '''
        web에서 받은거 , 저장해 놓고, 부를때 서버 찾아보고 없으면 추가
        param  : str 'YYYYMMDD'
        return : pd.DataFrame // 상장폐지종목도 포함됨
        '''
        print(f'get_상장법인정보_kind from KIND : {str_date}')

        def __get_from_kind_krx():
            '''
            kind.krx.co.kr 에서 받아옴
            return: df 
            '''

            import urllib

            DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'
            MARKET_CODE_DICT = {
                                    'kospi': 'stockMkt',
                                    'kosdaq': 'kosdaqMkt',
                                    'konex': 'konexMkt'
                                }
            
            def download_stock_codes(market=None, delisted=True): # delisted True -> 상장폐지종목 포함됨
                params = {'method': 'download'}

                if market.lower() in MARKET_CODE_DICT:
                    params['marketType'] = MARKET_CODE_DICT[market]

                if not delisted:
                    params['searchType'] = 13

                params_string = urllib.parse.urlencode(params)
                request_url = urllib.parse.urlunsplit(['http', DOWNLOAD_URL, '', params_string, ''])

                df = pd.read_html(request_url, header=0)[0]
                df.종목코드 = df.종목코드.map('{:06d}'.format)

                return df
                
            try:
                df_kospi = download_stock_codes('kospi', delisted=True)
                df_kosdaq = download_stock_codes('kosdaq',delisted=True)
                df_konex = download_stock_codes('konex', delisted=True)
                
                df_kospi['시장'] = '코스피'
                df_konex['시장'] = '코넥스'
                df_kosdaq['시장'] = '코스닥'
                
                df_master = pd.concat([df_konex, df_kosdaq, df_kospi])
                df_master['상장일'] = pd.to_datetime(df_master['상장일'])
                df_master['조회일'] = pd.datetime.today().strftime('%Y%m%d')
                
                return df_master
            except Exception as e:
                print(e, 'function __get_from_kind_krx')


        tablename = f'상장법인정보_{str_date}'
        try: # 먼저 table 읽어보고
            con  = self.get_db_stockmaster_con()
            df_tmp = pd.read_sql_table(tablename, con, index_col='index')
            print(len(df_tmp))

        except Exception as e: # 저장안된 경우, 저장하기
            print(e)
            print('fail to load from SQL')
            try:
                df_tmp = __get_from_kind_krx()
                if len(df_tmp) > 0 and to_sql:
                    df_tmp.to_sql(tablename, con, if_exists='replace')
                    print(f'uploaded at 서버 :{self.db_stockmaster_con} ')
                else:
                    print('목록없음 : maybe select another day')

            except Exception as e:
                print(e)

        return df_tmp

    def get_시총정보_네이버(self, str_date_start = str_recent_BDay, str_date_end =  str_recent_BDay):
        '''
        네이버 스크래핑 결과 받아오기, 종목코드가 없음

        return  : pd.DataFrame 시총정보
        '''
        print(f'from 네이버스크래핑 시총정보 from {str_date_start} to {str_date_end}')
        str_date_start = pd.to_datetime(str_date_start).strftime('%Y-%m-%d')
        str_date_end = pd.to_datetime(str_date_end).strftime('%Y-%m-%d')
        sql_시총정보_상승률_네이버 = \
            f"select * from 상승률_네이버 where 날짜 between '{str_date_start}' and '{str_date_end}'"
        try:
            df = pd.read_sql_query(sql_시총정보_상승률_네이버, self.db_trading_con)
            return df
        except Exception as e:
            print('error reading 상승률_네이버')
            print(e)

    def get_시총정보_krx(self, date=str_recent_BDay, to_sql=True, from_sql=True):
        '''
        web에서 받은거 , 저장해 놓고, 부를때 서버 찾아보고 없으면 추가
        그날의 정보 없는경우 (ex 휴일) 그냥 경고 and return None

        param  : str 'YYYYMMDD'
        return : pd.DataFrame // 날짜별 시총정보 들어있음
        '''        
        print(f'from marketdata  시총정보 on {date}')
        if not is_KRX_on_day(date):
            date = (pd.Timestamp(date) + BDay(1) - BDay(1)).strftime('%Y%m%d')
            print(f'휴일이므로 --> 그 {date} 로 ')
            return None

        def stock_master_price(date=None):
            if date == None:
                date = datetime.today().strftime('%Y%m%d')  # 오늘 날짜

            # tele_logger.debug('기준일:'+date+'_상장종목_시총_업데이트_시작')
            # STEP 01: Generate OTP
            gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
            gen_otp_data = {
                'name': 'fileDown',
                'filetype': 'xls',
                'url': 'MKD/04/0404/04040200/mkd04040200_01',
                'market_gubun': 'ALL',  # 시장구분: ALL=전체
                'indx_ind_cd': '',
                'sect_tp_cd': '',
                'schdate': date,
                'pagePath': '/contents/MKD/04/0404/04040200/MKD04040200.jsp',
            }

            r = requests.post(gen_otp_url, gen_otp_data, headers={})
            code = r.content  # 리턴받은 값을 아래 요청의 입력으로 사용.

            # STEP 02: download
            down_url = 'http://file.krx.co.kr/download.jspx'
            headers = {
                'Referer': 'http://marketdata.krx.co.kr/mdi',
                # 'Host' : 'marketdata.krx.co.kr'
            }
            down_data = {
                'code': code,
            }

            r = requests.post(down_url, down_data, headers=headers)
            df = pd.read_excel(BytesIO(r.content), header=0, thousands=',')

            if len(df) < 10:
                print.error('row가 10개보다 적음_휴장일?')
            return df

        tablename = f'시총정보_KRX_{date}'
        try: # 먼저 table 읽어보고
            con = self.get_db_stockmaster_con()
            df_tmp = pd.read_sql_table(tablename, con=con, index_col='index')
            print(len(df_tmp))

        except Exception as e:
            print(e)
            print('fail to load from SQL... try to fetch from marketdata')
            try:
                df_tmp = stock_master_price(date)
                if len(df_tmp) > 0 and to_sql:
                    df_tmp.to_sql(tablename, con, if_exists='replace')
                    print(f'uploaded at 서버 :{self.db_stockmaster_con} ')
                else:

                    print('목록없음 : maybe select another day')

            except Exception as e:
                print(e)
                print('목록없음 : maybe select another day')
                return None
        df_tmp['확인일자'] = date

        return df_tmp
    
    def get_시총정보_AWS_from_krx(self, str_date):
        tablename = f'시총정보_KRX_{str_date}'
        try: # 먼저 table 읽어보고
            con = self.get_db_stockmaster_con()
            df_tmp = pd.read_sql_table(tablename, con=con, index_col='index')
            print(len(df_tmp))
            return df_tmp
        except Exception as e:
            print(e)

    def get_시총정보_상장법인_krx_kind(self, date=str_recent_BDay, to_sql=False):
        '''
        merge 시총정보, 상장법인정보
        '''
        print(f'from marketdata_kind  시총정보 on {date}')
        if not is_KRX_on_day(date):
            print(f'{date} 휴일이므로 --> return None')
            return None
            # date = (pd.Timestamp(date) + BDay(1) - BDay(1)).strftime('%Y%m%d')
            # print(f'휴일이므로 --> 그 전 {date} 로 ')

        df_상장법인정보 = self.get_상장법인정보_kind(date, to_sql=to_sql)
        df_시총정보 = self.get_시총정보_krx(date, to_sql=to_sql)
        df_종목마스터_시총정보_일자별 = pd.merge(
            df_상장법인정보, df_시총정보,
            left_on = '종목코드', right_on='종목코드', how='inner'
        )

        # if len(df_종목마스터_시총정보_일자별) > 0 and to_sql:
        #     df_종목마스터_시총정보_일자별.to_sql(tablename, con, if_exists='replace')
        #     print(f'uploaded at 서버 :{self.db_stockmaster_con} ')
        return df_종목마스터_시총정보_일자별

    def get_시총정보_상장법인_네이버_kind(self, date=str_recent_BDay, to_sql=False):
        '''
        merge 시총정보, 상장법인정보
        '''
        print(f'from 네이버_kind 시총정보 on {date}')
        if not is_KRX_on_day(date):
            print(f'{date} 휴일이므로 --> return None')
            return None
            # date = (pd.Timestamp(date) + BDay(1) - BDay(1)).strftime('%Y%m%d')
            # print(f'휴일이므로 --> 그 전 {date} 로 ')

        df_상장법인정보 = self.get_상장법인정보_kind(date, to_sql=to_sql)
        df_시총정보 = self.get_시총정보_네이버(str_date_start = date, str_date_end = date)
        df_종목명_종목코드_시장 = self.get_종목명_종목코드_AWS()
        df_시총정보_종목코드 = pd.merge(
            df_시총정보, df_종목명_종목코드_시장[['종목명','종목코드']], 
            left_on = '종목명', right_on='종목명', how='inner', suffixes=("","_y")
        )
        df_종목마스터_시총정보_일자별 = pd.merge(
            df_상장법인정보, df_시총정보_종목코드,
            left_on = '종목코드', right_on='종목코드', how='inner'
        )

        # if len(df_종목마스터_시총정보_일자별) > 0 and to_sql:
        #     df_종목마스터_시총정보_일자별.to_sql(tablename, con, if_exists='replace')
        #     print(f'uploaded at 서버 :{self.db_stockmaster_con} ')
        return df_종목마스터_시총정보_일자별

    def get_종목코드_시장(self, str_date = str_recent_BDay):
                    '''
                    param  : str_date YYYYMMDD
                    return : dictionary key 종목코드, value 시장
                    '''
                    df = self.get_상장법인정보_kind(str_date)
                    return df.set_index('종목코드')[['시장']].to_dict()['시장']


db_util = Gb_Db()
# db_util_HC = gb_db(source='HC')

if __name__ == "__main__":
    # create index table in KWDB_DAILY
    if False:
        print('_' * 40)
        print('start create index table in KWDB_DAILY')
        df_all_stocks = db_util.get_stock_master_data()
        for stock_code in df_all_stocks['종목코드']:
            print(f'Update Code = {stock_code}')
            db_util.update_index_table_daily(stock_code)
        print('end create index table in KWDB_DAILY')
        print('-' * 40)

    if False:
        print('_' * 40)
        print('start create index table in KWDB_MINUTE')
        df_all_stocks = db_util.get_stock_master_data()
        for stock_code in df_all_stocks['종목코드']:
            print(f'Update Code = {stock_code}')
            db_util.update_index_table_minute(stock_code)
        print('end create index table in KWDB_MINUTE')
        print('-' * 40)

    df_result = db_util.get_sepcific_telegram_data(TELEGRAM_CHANNEL_ID_아이투자_텔레그램, order="asc", key_word="퀵리포트", limit=2)
    print(df_result.head())

    # df_minute = db_util.get_specific_minute_data("000020", start_date="2019-04-01", end_date="2019-04-02", order="ASC")
    # print(df_minute.head())
    # df_minute.to_csv("000020_2019-04-01.cvs")

    # df_test = db_util.get_종목코드_시장()
    # test = db_util.get_시총정보_krx()

