import pandas as pd
import numpy as np
import random
import sqlite3
from pandas_datareader import data
from datetime import datetime


class kpustockanalysis:
    now = (str(datetime.now().year) + '-' + str(datetime.now().month) + '-' + str(datetime.now().day))
    # 1. 지정된 마켓의 코드 데이터를 수집하여 데이터프레임을 생성하여 리턴
    @staticmethod
    def get_stockcode(country="US"):
        # 1.1. 한국
        if country is "KOREA" or country is 10:
            # 1.1.1. 한국 코드 데이터 미구현
            print("will be added in the future")
            return None

        # 1.2. 미국
        elif country is "US" or country is 20:
            # 1.2.1. 미국 코드 데이터를 웹에서 읽어옴
            ticker_list = pd.read_csv(
                "https://s3.amazonaws.com/quandl-production-static/end_of_day_us_stocks/ticker_list.csv")

            # 1.2.2. Ticker, Name 컬럼만 뽑아서 이름을 code, name으로 바꿈
            ticker_list = ticker_list[['Ticker', 'Name']]
            ticker_list.columns = ['code', 'name']

            # 1.2.3. 랜덤으로 20개 종목의 인덱스를 선정하는데, 2010년 데이터가 존재하는 것으로 한정
            ticker_index = set()
            random.seed(77)
            while len(ticker_index) != 20:
                i = random.choice(ticker_list.index)
                try:
                    data.DataReader(ticker_list.loc[i]['code'], 'yahoo', '2009-12-30', '2010-01-01')
                except:
                    pass
                else:
                    ticker_index.add(i)

            # 1.2.4. 선정된 인덱스를 통해 데이터 프레임을 생성 및 리턴
            ticker_list = ticker_list.loc[list(ticker_index)]
            ticker_list.index = range(1, len(ticker_list) + 1)

            return ticker_list

        # 1.3. 중국
        elif country is "CHINA" or country is 30:
            # 1.3.1. 중국 코드 데이터 미구현
            print("will be added in the future")
            return None

        # 1.4. 일본
        elif country is "JAPAN" or country is 40:
            # 1.4.1. 일본 코드 데이터 미구현
            print("will be added in the future")
            return None

    # 2. 주식 데이터를 읽어서 데이터 정제 및 리턴
    @staticmethod
    def get_dayprice(From, country, codes, startdate='2010-01-01', enddate=now, **kwargs):
        # 2.1. 원격사이트
        if From is 'remoteweb':
            # 2.1.1. 한국
            if country is "KOREA" or country is 10:
                # 2.1.1.1. 한국 데이터 미구현
                print("will be added in the future")
                return None
            # 2.1.2. 미국
            elif country is "US" or country is 20:
                # 2.1.2.1. 미국 주식 데이터
                us_stock_data_list = []
                us_stock_data = pd.DataFrame(columns=['code', 'name', 'date', 'open', 'close', 'high', 'low', 'volume', 'diff', 'rate'])
                for ticker, name in zip(codes['code'], codes['name']):
                    # 2.1.2.1.1. 미국 주식 데이터를 읽어옴
                    stock_data = data.DataReader(ticker, kwargs['site'], startdate, enddate)
                    stock_data = stock_data.reset_index('Date')
                    stock_data.columns = list(map(lambda s: s.lower(), stock_data.columns))
                    stock_data.index += 1

                    # 2.1.2.1.2. code, name 컬럼을 추가
                    stock_data['code'] = ticker
                    stock_data['name'] = name

                    # 2.1.2.1.3. 일간 종가 변화량을 계산하여 diff 컬럼 추가
                    stock_data['diff'] = stock_data['close'].diff()
                    stock_data = stock_data.fillna(0)

                    # 2.1.2.1.4. 일간 종가 변화율을 계산하여 rate 컬럼 추가
                    rate_list = [0]
                    for i in range(1, len(stock_data)):
                        rate = stock_data['diff'][i + 1] / stock_data['close'][i] * 100
                        rate_list.append(rate)
                    stock_data['rate'] = rate_list

                    # 2.1.2.1.5. 인덱스 재정렬
                    stock_data = stock_data[['code', 'name', 'date', 'open', 'close', 'high', 'low', 'volume', 'diff', 'rate']]
                    us_stock_data = pd.concat([us_stock_data, stock_data])

                    # 2.1.2.1.6. 주식 데이터를 리스트로 정리
                    us_stock_data_list.append(stock_data)

                # 2.1.2.3. 전체 데이터를 날짜순으로 정렬, 인덱스 재정리 후 데이터프레임 리턴
                us_stock_data = us_stock_data.sort_values(by='date', axis=0)
                us_stock_data.index = range(1, len(us_stock_data) + 1)

                print(us_stock_data['volume'])

                return us_stock_data
            # 2.1.3. 중국
            elif country is "CHINA" or country is 30:
                # 2.1.3.1. 중국 데이터 미구현
                print("will be added in the future")
                return None
            # 2.1.4. 일본
            elif country is "JAPAN" or country is 40:
                # 2.1.4.1. 일본 데이터 미구현
                print("will be added in the future")
                return None

        # 2.2. 로컬데이터베이스
        elif From is 'localdb':
            # 2.2.1. 한국
            if country is "KOREA" or country is 10:
                # 2.2.1.1. 한국 데이터 미구현
                print("will be added in the future")
                return None
            # 2.2.2. 미국
            elif country is "US" or country is 20:
                # 2.2.2.1. sqlite3에서 데이터를 가져옴
                if kwargs['dbms'] is 'sqlite3':
                    conn = sqlite3.connect("stock_data.db")
                    query = conn.execute("SELECT * FROM stock")
                    columns = [column[0] for column in query.description]
                    us_stock_data = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
                    conn.close()
                    us_stock_data = us_stock_data.set_index('index')
                    us_stock_data.index.names = ['']
                    return us_stock_data
                else:
                    return None
            # 2.2.3. 중국
            elif country is "CHINA" or country is 30:
                # 2.2.3.1. 중국 데이터 미구현
                print("will be added in the future")
                return None
            # 2.2.4. 일본
            elif country is "JAPAN" or country is 40:
                # 2.2.4.1. 일본 데이터 미구현
                print("will be added in the future")
                return None

        # 2.3. 로컬파일
        elif From is 'localfile':
            # 2.3.1. 한국
            if country is "KOREA" or country is 10:
                # 2.3.1.1. 한국 데이터 미구현
                print("will be added in the future")
                return None
            # 2.3.2. 미국
            elif country is "US" or country is 20:
                # 2.3.2.1. 파일에서 데이터를 가져옴
                filepath = kwargs['filepath'] + 'stock_data.' + kwargs['filetype']
                if kwargs['filetype'] is 'xlsx':
                    us_stock_data = pd.read_excel(filepath).set_index("Unnamed: 0")
                    us_stock_data.index.names = ['']
                    return us_stock_data
                elif kwargs['filetype'] is 'csv':
                    us_stock_data = pd.read_csv(filepath).set_index("Unnamed: 0")
                    us_stock_data.index.names = ['']
                    return us_stock_data
                else:
                    return None
            # 2.3.3. 중국
            elif country is "CHINA" or country is 30:
                # 2.3.3.1. 중국 데이터 미구현
                print("will be added in the future")
                return None
            # 2.3.4. 일본
            elif country is "JAPAN" or country is 40:
                # 2.3.4.1. 일본 데이터 미구현
                print("will be added in the future")
                return None

    # 3. 데이터베이스의 코드데이터 및 일간주가 데이터를 최신화
    @staticmethod
    def db_refresh(df, session, country, code, startdate="2010.1.1", endate=now):
        # 3.1. 한국
        if country is "KOREA" or country is 10:
            print("will be added in the future")
            return None

        # 3.2. 미국
        elif country is "US" or country is 20:
            # 3.2.1. 로컬데이터베이스의 모든 데이터를 가져옴
            query = session.execute("SELECT * FROM stock")
            columns = [column[0] for column in query.description]
            db_data = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
            db_data = db_data.set_index('index')
            db_data.index.names = ['']

            # 3.2.2. 두 데이터의 row 를 비교하여 차이가 있으면 db 갱신
            diff_count = abs(len(df) - len(db_data))
            if diff_count > 0:
                df.to_sql('stock', session, if_exists="replace")

            return diff_count
        # 3.3. 중국
        elif country is "CHINA" or country is 30:
            # 3.3.1. 중국 데이터 미구현
            print("will be added in the future")
            return None
        # 3.4. 일본
        elif country is "JAPAN" or country is 40:
            # 3.4.1. 일본 데이터 미구현
            print("will be added in the future")
            return None

    # 4. 수집/저장된 데이터 검증
    @staticmethod
    def verify_dbdata(session, country="US", startdate='2010-01-01', enddate=now):
        # 4.1. 한국
        if country is "KOREA" or country is 10:
            # 4.1.1. 한국 데이터 미구현
            print("will be added in the future")
            return None
        # 4.2. 미국
        elif country is "US" or country is 20:
            # 4.2.1. 로컬데이터베이스에서 특정날짜의 데이터를 가져옴
            query = session.execute("SELECT * FROM stock where date between '" + startdate + "' and '" + enddate + "'")
            columns = [column[0] for column in query.description]
            us_stock_data = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
            us_stock_data = us_stock_data.set_index('index')
            us_stock_data.index.names = ['']

            # 4.2.2 None 값이 있는지 확인
            if us_stock_data.isnull().any().any():
                print("There are NaN values in the data")
                return None

            # 4.2.3 거래일 동일 체크
            value_count = us_stock_data['code'].value_counts()
            max_count = value_count.max()
            err_list = []
            for code in us_stock_data['code'].value_counts().index:
                if value_count[code] != max_count:
                    err_list.append(code)

            if err_list:
                print(str(err_list) + " are not the correct quantity")
                return None

            # 4.2.3. 오류가 없을 경우 데이터를 정제
            refined_us_stock_data = pd.DataFrame(np.zeros((20, 9)),
                                                 index=range(1, 21),
                                                 columns=['code', 'name', 'rows', 'startdate', 'enddate', 'close_max', 'close_min', 'close_avr', 'volume_avr'])
            refined_us_stock_data['code'].astype('str')
            refined_us_stock_data['name'].astype('str')
            index = 1

            codes = list(set(us_stock_data['code']))
            names = list(set(us_stock_data['name']))

            for name in names:
                stock_data = us_stock_data[us_stock_data['name'] == name]
                stock_data = stock_data.sort_values(by='date', axis=0)
                stock_data.index = range(1, len(stock_data) + 1)
                refined_us_stock_data.loc[index]['rows'] = len(stock_data)
                refined_us_stock_data.loc[index]['startdate'] = stock_data.loc[1]['date']
                refined_us_stock_data.loc[index]['enddate'] = stock_data.loc[len(stock_data)]['date']
                refined_us_stock_data.loc[index]['close_max'] =  stock_data['close'].max()
                refined_us_stock_data.loc[index]['close_min'] = stock_data['close'].min()
                refined_us_stock_data.loc[index]['close_avr'] = stock_data['close'].mean()
                refined_us_stock_data.loc[index]['volume_avr'] = stock_data['volume'].mean()
                index += 1

            refined_us_stock_data['code'] = codes
            refined_us_stock_data['name'] = names

            return refined_us_stock_data
        # 4.3. 중국
        elif country is "CHINA" or country is 30:
            # 4.3.1. 중국 데이터 미구현
            print("will be added in the future")
            return None
        # 4.4. 일본
        elif country is "JAPAN" or country is 40:
            # 4.4.1. 일본 데이터 미구현
            print("will be added in the future")
            return None
