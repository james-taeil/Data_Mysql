import DBMarket
mk = DBMarket.DBMarket()
print(mk.get_daily_price('삼성전자', '2020-09-01', '2020-09-24'))
print(mk.get_daily_price('삼양홀딩스', '2020-09-01', '2020-09-24'))