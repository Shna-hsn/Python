import cx_Oracle as cx

connection = cx.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
sql = 'SELECT * FROM TEMP_NANDER'
cursor = connection.cursor()
cursor.execute(sql)
data = cursor.fetchall()
print(data[0][0])