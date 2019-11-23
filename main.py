from kpu2019.kpustockanalysis import kpustockanalysis
import pandas as pd
from datetime import datetime
import sqlite3
import os

# startdate, enddate 할당
now = datetime.now()
startdate = '2010-01-01'
enddate = str(now.year) + '-' + str(now.month) + '-' + str(now.day)

# ticker를 db에서 읽어옴
conn = sqlite3.connect('stock_data.db')
query = conn.execute("SELECT * FROM code")
columns = [column[0] for column in query.description]
ticker_list = pd.DataFrame.from_records(data=query.fetchall(), columns=columns)
conn.close()
ticker_list = ticker_list.set_index('index')
ticker_list.index.names = ['']

flag = 201781580076

# 미국 주식 데이터 분석 프로그램 menu 출력
while flag != '0':
    print(">> 미국 주식 데이터 분석 <<\n1. 코드 데이터 수집\n2. Yahoo Finance를 통한 데이터 수집"
          "\n3. xlsx 파일로 저장\n4. csv 파일로 저장\n5. DB 파일로 저장\n6. xlsx 파일 읽어오기"
          "\n7. csv 파일 읽어오기\n8. DB 파일 읽어오기\n9. 데이터 검증\n0. 프로그램 종료")
    flag = input(">> ")
    if flag == '1':
        # 1. 미국 코드 데이터 수집
        ticker_list = kpustockanalysis.get_stockcode('US')
        print(ticker_list)
    elif flag == '2':
        # 2. Yahoo Finance를 통한 미국 주식 데이터 수집
        us_stock_data = kpustockanalysis.get_dayprice('remoteweb', 'US', ticker_list, startdate, enddate,
                                                      site='yahoo')
        print(us_stock_data)
    elif flag == '3':
        # 3. xlsx 파일로 저장
        us_stock_data.to_excel('stock_data.xlsx')
        print("stock_data.xlsx 파일로 저장되었습니다.")
    elif flag == '4':
        # 4. csv 파일로 저장
        us_stock_data.to_csv('stock_data.csv')
        print("stock_data.csv 파일로 저장되었습니다.")
    elif flag == '5':
        # 5. DB 파일로 저장
        if os.path.isfile('stock_data.db'):
            conn = sqlite3.connect('stock_data.db')
            update_count = kpustockanalysis.db_refresh(us_stock_data, conn, "US", ticker_list, startdate, enddate)
            ticker_list.to_sql('code', conn, if_exists="replace")
            conn.close()
            print("stock_data.db 파일에 %d개의 데이터가 업데이트 되었습니다." % update_count)
        # db 파일이 존재하지 않을 경우에는 생성
        else:
            conn = sqlite3.connect('stock_data.db')
            us_stock_data.to_sql('stock', conn, if_exists="replace", )
            ticker_list.to_sql('code', conn, if_exists="replace")
            conn.close()
            print("stock_data.db 파일로 저장되었습니다.")
    elif flag == '6':
        # 6. xlsx 파일 읽어오기
        us_stock_data = kpustockanalysis.get_dayprice('localfile', 'US', ticker_list, startdate='2010-01-01', enddate=now, filepath='', filetype='xlsx')
        us_stock_data.index.names = ['']
        print(us_stock_data)
    elif flag == '7':
        # 7. csv 파일 읽어오기
        us_stock_data = kpustockanalysis.get_dayprice('localfile', 'US', ticker_list, startdate='2010-01-01', enddate=now, filepath='', filetype='csv')
        us_stock_data.index.names = ['']
        print(us_stock_data)
    elif flag == '8':
        # 8. DB 파일 읽어오기
        conn = sqlite3.connect("stock_data.db")
        us_stock_data = kpustockanalysis.get_dayprice('localdb', 'US', ticker_list, startdate='2010-01-01', enddate=now, dbms='sqlite3')
        conn.close()
        print(us_stock_data)
    elif flag == '9':
        # 9. 데이터 검증
        conn = sqlite3.connect('stock_data.db')
        refined_us_stock_data = kpustockanalysis.verify_dbdata(conn, "US", startdate, enddate)
        conn.close()
        print(refined_us_stock_data)
    elif flag == '0':
        print('프로그램을 종료합니다.')
