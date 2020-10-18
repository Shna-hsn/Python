import sys,importlib
import datetime,time
import os,happybase
from impala.dbapi import connect
from hdfs import *
from hdfs import InsecureClient
import cx_Oracle
reload(sys)
sys.setdefaultencoding('utf-8')

def getfilesn(aoi_file):
    filesn = aoi_file.split('@')[-1].split('.')[0].split('-')[-1].replace(".JPG","")
    return filesn



def getnewmodel():
    # conn=cx_Oracle.connect('SFCFA139/SFCFA139@10.41.129.33/orcl')
    modellist = []
    sql="SELECT DISTINCT MODEL FROM SFCMODEL WHERE MODEL LIKE 'X17%' OR MODEL LIKE 'X19%'"
    try:
        conn=cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
        curs=conn.cursor()
        rr=curs.execute(sql)
        result=curs.fetchall()
        for i in range(len(result)):
            modellist.append(result[i][0])
        curs.close()
        conn.close()
    except Exception as e:
        print e
    return modellist

def getoldmodel():
    # conn=cx_Oracle.connect('SFCFA139/SFCFA139@10.41.129.33/orcl')
    modellist = []
    sql="SELECT DISTINCT MODEL FROM SFCMODEL WHERE MODEL not LIKE 'X17%' and not MODEL LIKE 'X19%'"
    try:
        conn=cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
        curs=conn.cursor()
        rr=curs.execute(sql)
        result=curs.fetchall()
        for i in range(len(result)):
            modellist.append(result[i][0])
        curs.close()
        conn.close()
    except Exception as e:
        print e
    return modellist
    
def rename(aoi_jpg):
    new_name = aoi_jpg.replace(' ','')
    return new_name


def delJPG_Newmodel(basepath):
    now_time = datetime.datetime.now()
    now_date_str = now_time.strftime('%Y%m%d')
    now_date = datetime.datetime.strptime(now_date_str,'%Y%m%d')
    try:
        # client = Client('http://10.41.158.72:50070')

        # client = InsecureClient('http://10.41.158.106:50075', user='hadoop')
        client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
        # path="/P8AOI"
        # path1="C:/Users/z18073048/Desktop/bigdata/X1778-ANSI-BOT_20200813_TB1-F11-TRI-05@20200813094718-FPW03354EX3P49WBS.JPG"
        # client.upload(path,path1,cleanup=True)
        folderlist = client.list(basepath)
        newmodel = getnewmodel()
        for i in range(len(folderlist)):
            if isinstance(folderlist[i],unicode):
            #if isinstance(folderlist[i],list):
                folderlist[i] = folderlist[i].decode('string_escape')
            fname=folderlist[i]
            #print folderlist[i]
            #if  (fname=='X1777' or fname=='X1778' or fname=='Errormodel'):
            if  (fname in newmodel):
                folderlist1=client.list(basepath+'/'+fname)
                print 'newmodel:',folderlist[i],folderlist1
                for i in range(len(folderlist1)):
                    if isinstance(folderlist1[i],list):
                        folderlist1[i] = folderlist1[i].decode('string_escape')
                    date_flag = is_valid_date(folderlist1[i])
                    #print date_flag
                    if date_flag == 'true':
                        folderItem = datetime.datetime.strptime(folderlist1[i],'%Y%m%d')
                        if folderItem + datetime.timedelta(days=365) <= now_date:
                            paths = basepath +fname+ '/'+ folderlist1[i]
                            delHbase(folderlist1[i],client,paths)
                            deleteKudu(folderlist1[i],client,paths)

                            try:
                                client.delete(paths,recursive=True)
                                print paths +' is delete'

                            except Exception as e:
                                print e
    except Exception as e:
        print e



def delJPG_Oldmodel(basepath):
    now_time = datetime.datetime.now()
    now_date_str = now_time.strftime('%Y%m%d')
    now_date = datetime.datetime.strptime(now_date_str,'%Y%m%d')
    try:
        # client = Client('http://10.41.158.72:50070')

        # client = InsecureClient('http://10.41.158.106:50075', user='hadoop')
        client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
        # path="/P8AOI"
        # path1="C:/Users/z18073048/Desktop/bigdata/X1778-ANSI-BOT_20200813_TB1-F11-TRI-05@20200813094718-FPW03354EX3P49WBS.JPG"
        # client.upload(path,path1,cleanup=True)
        folderlist = client.list(basepath)
        oldmodel = getoldmodel()
        for i in range(len(folderlist)):
            if isinstance(folderlist[i],unicode):
            #if isinstance(folderlist[i],list):
                folderlist[i] = folderlist[i].decode('string_escape')
            fname=folderlist[i]
            #print folderlist[i]
            #if  (fname=='X1777' or fname=='X1778' or fname=='Errormodel'):
            if  (fname in oldmodel):
                folderlist1=client.list(basepath+'/'+fname)
                print 'oldmodel:',folderlist[i],folderlist1
                for i in range(len(folderlist1)):
                    if isinstance(folderlist1[i],list):
                        folderlist1[i] = folderlist1[i].decode('string_escape')
                    date_flag = is_valid_date(folderlist1[i])
                    #print date_flag
                    if date_flag == 'true':
                        folderItem = datetime.datetime.strptime(folderlist1[i],'%Y%m%d')
                        if folderItem + datetime.timedelta(days=185) <= now_date:
                            paths = basepath +fname+ '/'+ folderlist1[i]
                            delHbase(folderlist1[i],client,paths)
                            deleteKudu(folderlist1[i],client,paths)
                            try:
                                client.delete(paths,recursive=True)
                                print  paths +' is delete'
                            except Exception as e:
                                print e
    except Exception as e:
        print e
        
def is_valid_date(str):
    try:
        time.strptime(str, "%Y%m%d")
        return 'true'
    except:
        return 'false'

def delHbase(folderitem,client,paths):
    #client = Client('http://10.55.13.61:50070')
    print "deleteHbase:"
    try:
        fileslist = client.list(paths)
    except Exception as e:
        print e
    if fileslist !=[]:
        connt = happybase.Connection('10.41.158.65')
        table = connt.table('ie4_p8_aoi')
        for i in range(len(fileslist)):
        	#2.7.11 
        	#if isinstance(fileslist[i],Unicode):
            if isinstance(fileslist[i],list):
                fileslist[i] = fileslist[i].decode('string_escape')
            strlist = fileslist[i].split('@')[-1].split('.')[0].split('-')
            if len(strlist) == 2:
                row_key = strlist[1]
                value = strlist[0]
                key_value = 'cf:' + value
                columnlist = []
                columnlist.append(key_value)
                try:
                    table.delete(row_key,columns=columnlist)
                    for x in range(len(columnlist)):
                        print  row_key +','+columnlist[x] + ' is delete'
                except Exception as e:
                    print e
                    continue
            else:
                print fileslist[i] + ' file name is error!'
        #table.delete('row-key')
        #table.delete('row-key', columns=['cf1:col1', 'cf1:col2'])

def deleteKudu(kudu_filelist,client,paths):
    print "deleteKudu:"
    tablename ="allie.aoi_imageurl"
    columnitems = "(usn,trndate,imageurl)"
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'

    port = 21050
    valueslist_str =' '
    valuelist=' '

    fileslist=client.list(paths)
    for i in range(len(fileslist)):
        aoi_file = fileslist[i]
        # aoi_file.decode('string_escape')
        aoi_file = rename(aoi_file)

        sn=getfilesn(aoi_file)
        print "deleteKudu: " +aoi_file
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
    sql = "delete from "+ tablename +" where usn in (" + valueslist_str +")"
    print sql
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
            print e
        conn.close()



if __name__ == "__main__":
    basepath = '/P8AOI/MapData/'
    delJPG_Newmodel(basepath)
    delJPG_Oldmodel(basepath)

