import sys,importlib
import datetime,time
import os,happybase
from impala.dbapi import connect
from hdfs import *
from hdfs import InsecureClient
import cx_Oracle

def getfilesn(aoi_file):
    filesn = aoi_file.split('@')[-1].split('.')[0].split('-')[-1].replace(".JPG","")
    return filesn

def rename(aoi_jpg):
    new_name = aoi_jpg.replace(' ','')
    return new_name

def selectKudu():
    countusn = 0
    client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
    print("selectKudu:")
    tablename ="allie.aoi_imageurl"
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'

    port = 21050
    valueslist_str =' '
    valuelist=' '

    fileslist=client.list('/P8AOI/MapData/X1726/20210322')
    for i in range(len(fileslist)):
        countusn += 1
        aoi_file = fileslist[i]
        aoi_file = rename(aoi_file)
        sn=getfilesn(aoi_file)
        # print("deleteKudu: " +aoi_file)
        if len(sn) > 10:
            if '@D@' in aoi_file:
                dt = aoi_file[aoi_file.find('@')+3:aoi_file.find('@')+11]
            elif '@' in aoi_file:
                dt = aoi_file[aoi_file.find('@')+1:aoi_file.find('@')+9]
            valuelist='\''+sn+'\''
        if i==0:
            valueslist_str = valuelist
        else:
            valueslist_str += ',' + valuelist
        if (countusn >= 8000) or (i == (len(fileslist) - 1)):
            sql = "select * from "+ tablename +" where usn in (" + valueslist_str +")"
            os.system('echo ' + sql + ' >> C:/Users/Z18073047/Desktop/sql.txt')
            valueslist_str = "''"
            countusn = 0
            try:
                conn = connect(host=hostname1,port=port)
            except:
                time.sleep(2)
            try:
                conn = connect(host=hostname2,port=port)
            except:
                time.sleep(2)
                conn = connect(host=hostname3,port=port)
            finally:
                try:
                    cur = conn.cursor()
                    cur.execute(sql)
                except Exception as e:
                    print(e)
                conn.close()

if __name__ == "__main__":
    selectKudu()
