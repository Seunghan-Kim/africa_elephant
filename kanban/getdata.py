import pandas as pd
import datetime as dt
from gb_db import Gb_Db

try:
    db_YE = Gb_Db(source='YE')
    con_YE_daily_naver = db_YE.get_db_daily_naver_con()
except:
    print('cannot get con_YE_daily_naver')
    pass

try:
    db_HC = Gb_Db(source='HC')
    con_HC = db_HC.get_db_trading_con()
except:
    print('cannot get db_HC')
    pass
try:
    db_AWS = Gb_Db(source='AWS')
    con_AWS = db_AWS.get_db_trading_con()
except:
    print('cannot get db_AWS')
    pass
#

def getdata(date):

    str_date = pd.Timestamp(date).strftime('%Y%m%d')
    table_name = f'{str_date}_daily_allstock_naver'


    try:
        df = pd.read_sql_query(f'select * from {table_name} order by 등락률 DESC LIMIT 30', con_YE_daily_naver)
        return df

    except Exception as e:
        # pymysql.err.ProgrammingError
        print(e)
        # df = pd.DataFrame([['2019-11-22', '네이버', 20, 'kospi', -10.1, 10000],
        #                     ['2019-11-22', '다음', 10, 'kosdaq', -5.2, 5000], 
        #                     ['2019-11-22', '구글', 3, 'kospi', 55.3, 8000], 
        #                     ['2019-11-22', '아프리카코끼리', 30, 'kosdaq', 102.3, 7000]
        #                                             ], columns=['date', '종목명', '등락률', '시장', '누적수익률', '시가총액']
        #                                         )
        # df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

        # # str_date = date.strftime('%Y-%m-%d')
        # df_sel = df[df['date'] == str_date]

        # return df_sel