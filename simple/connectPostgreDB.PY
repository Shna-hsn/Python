import psycopg2

def getpostgrate():
    PG_CONF = {
        'database':'SABUPRD',
        'user':'FATPPRD',
        'password':'FATPPRD',
        'host':'10.41.158.156',
        'port':'5432'
    }
    conn = psycopg2.connect(**PG_CONF)
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print(rows)
    cursor.close()
    conn.close()

if __name__ == '__main__':
    sql = 'select * from allie_albarcode'
    getpostgrate()