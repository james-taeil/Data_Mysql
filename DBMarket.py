import pandas as pd
import pymysql
from datetime import datetime
from datetime import timedelta
import re


class DBMarket:
    def __init__(self):
        """생성자 : MySQL 연결 및 종목 코드 딕셔너리 생성"""

        self.conn = pymysql.connect(host='localhost',
                                    # port=3306,
                                    db='naverfinance',
                                    user='root',
                                    passwd='1234',
                                    # autocommit=True,
                                    charset="utf8")
        self.codes = {}
        self.get_comp_info()

    def __del__(self):
        """소멸자 : MySQL 연결 해제"""
        self.conn.close()

    def get_comp_info(self):
        """company_info 테이블에서 읽어와서 codes 에 저장"""
        sql = "select * from company_info"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            self.codes[krx['code'].values[idx]] =  krx['company'].values[idx]






    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목의 일별 시세를 데이터프레임 형태로 반환
            - code : KRX 종목('005930') 또는 상장기업명('삼성전자')
            - start_date : 조회 시작일 2020-01-01 미 입력시 1년전 오늘
            - end_date : 조회 종료일 2020-12-31 미 입력시 오늘 날짜
        """
        if start_date is None:
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        else:
            start_lst = re.split('\D+', start_date)
            if start_lst[0] == '':
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])
            if start_year < 1900 or start_year > 2200:
                print(f'ValueError: start_month({start_year:d} is wrong.')
                return
            if start_month < 1 or start_month > 12:
                print(f'ValueError: start_month({start_month:d} is wrong.')
                return
            if start_day < 1 or start_day > 31:
                print(f'ValueError: start_day({start_day:d} is wrong.')
                return
            start_date = f'{start_year:04d}-{start_month:02d}-{start_day:02d}'

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print("end_date is initialized to '{}'".format(end_date))
        else:
            end_lst = re.split('\D+', end_date)
            if end_lst[0] == '':
                end_lst = end_lst[1:]
            end_year = int(end_lst[0])
            end_month = int(end_lst[1])
            end_day = int(end_lst[2])

            if end_year < 1800 or end_year > 2200:
                print(f'ValueError: end_year({end_year:d} is wrong.')
                return
            if end_month < 1 or end_month > 12:
                print(f'ValueError: end_month({end_month:d} is wrong.')
                return
            if end_day < 1 or end_day > 31:
                print(f'ValueError: end_day({end_day:d} is wrong.')
                return
            end_date = f'{end_year:04d}-{end_month:02d}-{end_day:02d}'

        #딕셔너리에 값으로 키를 조회
        codes_keys = list(self.codes.keys())     #딕셔너리 키 리스트 생성
        codes_values = list(self.codes.values()) #딕셔너리 값 리스트 생성


        #사용자가 입력한 code가 키 리스트에 있으면 그대로 사용
        if code in codes_keys:
            pass

        #사용자가 입력한 code가 없으면 인덱스를 구한 뒤 키 리스트에서 동일한 인덱스에 키를 찾기
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_keys[idx]
        else:
            print(f"ValueError: Code({code}) doesn`t exist.")

            #판다스의 read_sql 함수 사용해 select 결과 데이터 프레임에 가져오면 정수형 인덱스 별도 생성
        sql = f"select * from daily_price where code = '{code}' and date >= '{start_date}'" \
              f"and date = '{end_date}'"

        df = pd.read_sql(sql, self.conn)

        #데이터프레임의 인덱스를 date컬럼으로 새로 성정
        df.index = df['date']
        return df


# if __name__ == '__main__':
#     a = DBMarket()
#     a.get_daily_price('삼성전자', '2020-09-14', '2020-09-23')