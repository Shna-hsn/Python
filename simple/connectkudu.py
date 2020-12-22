import time
from impala.dbapi import connect

def connectkudu():
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'
    port = 21050
    try:
        conn = connect(host = hostname1,port = port)
    except:
        time.sleep(2)
    try:
        conn = connect(host = hostname2,port = port)
    except:
        time.sleep(2)
        conn = connect(host = hostname3,port = port)
    finally:
        sql = "select * from allie.aoi_ht where usn = 'FPW0517G0SJQ1GQAU'"
        try:
            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            print(result)
        except Exception as e:
            print(e)

connectkudu()