import pymysql

connection  = pymysql.connect(host='localhost',
                              port=3306,
                              db='naverfinance',
                              user='root',
                              passwd='1234',
                              autocommit=True)
cursor = connection.cursor()
cursor.execute("SELECT VERSION();")
result = cursor.fetchone()

print("접속 성공 MySQL version : {}".format(result))

connection.close()