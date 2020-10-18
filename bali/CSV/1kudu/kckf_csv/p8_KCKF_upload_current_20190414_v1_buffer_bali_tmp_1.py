# -*- coding:utf8 -*-
import sys,os,csv,shutil,datetime,time
import pandas as pd
from impala.dbapi import connect

reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'

pid_folder = '/home/armap/kuduprogram/p8_KCKF_upload_data_buffer_bali_tmp/'
pid_id = 1

def writePid():
    pid = str(os.getpid())
    f = open(pid_folder+'KCKFPID_buffer_bali_tmp_'+str(pid_id)+'.pid', 'a+')
    f.write(pid+'\n')
    f.close()
    return pid

def readPidCount():
    with open(pid_folder+'KCKFPID_buffer_bali_tmp_'+str(pid_id)+'.pid', 'r') as f:
        lines = f.readlines()
        count = len(lines)
    return count

def delPid(pid):
    with open(pid_folder+'KCKFPID_buffer_bali_tmp_'+str(pid_id)+'.pid', 'r') as f:
        lines = f.readlines()
    with open(pid_folder+'KCKFPID_buffer_bali_tmp_'+str(pid_id)+'.pid', 'w') as w_f:
        for line in lines:
            if pid in line:
                continue
            w_f.write(line)

def moveFile(sourcefile,destifile):
    if os.path.exists(sourcefile):
        try:
            shutil.move(sourcefile,destifile)
        except Exception,e:
            print e

def get_FileList(folderpath):
    kn_filelist =[]
    kckf_filelist=[]
    pr_filelist = []
    kckf_r_filelist =[]
    isakf_filelist = []
    pidcount = readPidCount()
    if pidcount == 1:
        fileslist = os.listdir(folderpath)
    else:
        fileslist = []
        fileslisttemp = os.listdir(folderpath)
        for filename in fileslisttemp:
            filepath = folderpath + os.sep + filename
            filecreatetime_stamp = datetime.datetime.fromtimestamp(os.stat(filepath).st_ctime)
            filecreatetime_str = datetime.datetime.strftime(filecreatetime_stamp,'%Y%m%d %H:%M:%S')
            filecreatetime = datetime.datetime.strptime(filecreatetime_str,'%Y%m%d %H:%M:%S')
            if filecreatetime > datetime.datetime.now() - datetime.timedelta(minutes=2):
                fileslist.append(filename)
    #fileslist = os.listdir(folderpath)
    errorfolderdt = errorfolder + os.sep + now_date
    if not os.path.exists(errorfolderdt):
        os.mkdir(errorfolderdt)
    for i in range(len(fileslist)):
        if 'KEY-NOISE' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kn_filelist.append(fileslist[i])
            else:
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'RUBBER' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                pr_filelist.append(fileslist[i])
            else:
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'PRE-KEY-FORCE' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                isakf_filelist.append(fileslist[i])
            else:
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'BACK' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kckf_r_filelist.append(fileslist[i])
            else:
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        else:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kckf_filelist.append(fileslist[i])
            else:
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
    return kn_filelist,kckf_filelist,pr_filelist, kckf_r_filelist,isakf_filelist

def impalaConnection(hostname,port):
    hostname1 = 'p8cdhdatap03.wzs.wistron'
    hostname2 = 'p8cdhdatap04.wzs.wistron'
    try:
        conn = connect(host=hostname, port=port)
    except:
        time.sleep(2)
        try:
            conn = connect(host=hostname1, port=port)
        except:
            time.sleep(2)
            conn = connect(host=hostname2, port=port)
    finally:
        return conn

def close_Connection(conn):
    conn.close()

def dataframeInstance(filename):
    filepath = file_folder + os.sep + filename
    sourcefile = filepath
    destifile = errorfolder+os.sep+now_date+os.sep+filename
    try:
        filedataframetemp = pd.read_csv(filepath)
        fileheadertemp = [item for item in filedataframetemp.columns.values if 'Unnamed:' not in item]
        filedataframetemp1 = pd.DataFrame(filedataframetemp,columns=fileheadertemp)
        fileheader = [item.strip() for item in filedataframetemp1.columns.values]
        filedataframe =pd.DataFrame(filedataframetemp1.values,columns=fileheader)
        filedataframe.dropna(axis=0, how='all')
        if (True in pd.isnull(filedataframe['Test_Start_Time']).values) or (True in pd.isnull(filedataframe['SN']).values) or(True in pd.isnull(filedataframe['Key_Name']).values) \
                or (True in pd.isnull(filedataframe['Location']).values) or ('NOREAD' in filedataframe['SN'].values):
            moveFile(sourcefile,destifile)
            filedataframe = {}
            fileheader = []
            print 'there are null value in primary_key columns '
        #return filedataframe,fileheader
    except Exception,e:
        #shutil.move(filepath,errorfolder+os.sep+now_date+os.sep+filename)
        moveFile(sourcefile,destifile)
        print e
        print filepath
        filedataframe = {}
        fileheader = []
    finally:
        return filedataframe,fileheader

def headermapping(func):
    conn = impalaConnection(hostname,port)
    cur = conn.cursor()
    sql = "select csvfileheader,kudutablecolumn from allie.csv_columnmapping where functionname=" +'\''+str(func)+'\''
    cur.execute(sql)
    itemslist = cur.fetchall()
    header_dict = dict(itemslist)
    close_Connection(conn)
    return header_dict

def add_tableItem(func,tablename,itemlist,header_dict):
    #columns = ''
    #i = 1
    OkFile_flag = 'True'
    conn = impalaConnection(hostname,port)
    cur = conn.cursor()
    for itemtemp in itemlist:
        columns = ''
        item = ''
        mapvaluelist =[]
        mapvaluelist.append(func)
        mapvaluelist.append(itemtemp)
        if ('_' in itemtemp) and ('.' in itemtemp):
            itemtemplist = itemtemp.split('_')
            for itemsub in itemtemplist:
                if itemsub !='':
                    if len(itemsub) >=2:
                        item += itemsub[0:1]
                    else:
                        item += itemsub[0]
                    item += itemsub[0]
            if '(+)' in itemtemp:
                item += 'plus'
            if '(-)' in itemtemp:
                item += 'minus'
            item += itemtemp[itemtemp.find('.')+1]
        elif ('_' in itemtemp) and ('.' not in itemtemp):
            itemtemplist = itemtemp.split('_')
            for itemsub in itemtemplist:
                if itemsub !='':
                    if len(itemsub) >=2:
                        item += itemsub[0:1]
                    else:
                        item += itemsub[0]
            if '(+)' in itemtemp:
                item += 'plus'
            if '(-)' in itemtemp:
                item += 'minus'
        elif ('_' not in itemtemp) and ('.' in itemtemp):
            item += itemtemp[:itemtemp.find('.')]
            item += itemtemp[itemtemp.find('.')+1]
        else:
            item += itemtemp
        item = item.lower()
        flag = True
        while flag:
            if item not in header_dict.values():
                flag = False
            else:
                item +='u'
        mapvaluelist.append(item)
        if i < len(itemlist):
            columns += item + ' STRING,'
            i = i + 1
        else:
            columns += item + ' STRING'
        sql = "select max(headerseq) from allie.csv_columnmapping where functionname=" + '\''+str(func)+'\''
        cur.execute(sql)
        maxheaderseq = cur.fetchall()
        if maxheaderseq !=[]:
            headerseq = int(maxheaderseq[0][0]) + 1
        else:
            headerseq = 1
        mapvaluelist.append(str(func+'_'+itemtemp))
        mapvaluelist.append(headerseq)
        mapvaluelist = str(mapvaluelist).replace('[','(').replace(']',')')
        #sql1 = "insert into allie.csv_columnmapping(functionname,csvfileheader,kudutablecolumn,downloadcolumn,headerseq)" + " values" + mapvaluelist
        #cur.execute(sql1)
        columns = '(' + columns +')'
        try:
            sql1 = "ALTER TABLE " + tablename +" ADD COLUMNS " + columns
            cur.execute(sql1)
        except Exception,e:
            OkFile_flag = 'False'
            print e
            continue
        sql2 = "insert into allie.csv_columnmapping(functionname,csvfileheader,kudutablecolumn,downloadcolumn,headerseq)" + " values" + mapvaluelist
        cur.execute(sql2)
    close_Connection(conn)
    return OkFile_flag

def dataUpload(fileindex,fileheader,header_dict,tablename,filedataframe,filename):
    conn = impalaConnection(hostname,port)
    cur = conn.cursor()
    valueslist_str =''
    columnitems = '(' +'lastrecordflag,'
    for item in fileheader:
            columnitems +=header_dict[item] +','
    columnitems +='trndate' + ')'
    time1 = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(time1,'%Y-%m-%d %H:%M:%S')
    for i in fileindex:
        if i == 0:
            sql1 = "select teststarttime from " + tablename + " where usn=%s and keyname=%s and keylocation=%s and lastrecordflag=1"
            values = [str(filedataframe.loc[i,'SN']),str(filedataframe.loc[i,'Key_Name']),filedataframe.loc[i,'Location']]
            cur.execute(sql1,values)
            testtimevalue = cur.fetchall()
            if (testtimevalue != []):
                if '-' in testtimevalue[0][0]:
                    testtimevaluetemp = testtimevalue[0][0].replace('-','/')
                    kudu_testtimestamp = datetime.datetime.strptime(testtimevaluetemp,'%Y/%m/%d %H:%M:%S')
                else:
                    kudu_testtimestamp = datetime.datetime.strptime(testtimevalue[0][0],'%Y/%m/%d %H:%M:%S')
                test_start_time = filedataframe.loc[i,'Test_Start_Time']
                #print test_start_time
                if '-' in test_start_time:
                    test_start_time = test_start_time.replace('-','/')
                if len(test_start_time)>15:
                    csv_testtimestamp = datetime.datetime.strptime(test_start_time,'%Y/%m/%d %H:%M:%S')
                else:
                    csv_testtimestamp = datetime.datetime.strptime(test_start_time,'%Y/%m/%d %H:%M')
                if csv_testtimestamp > kudu_testtimestamp:
                    sql2 = "update " + tablename + " set lastrecordflag=0 where usn=%s  and lastrecordflag =1"
                    values1 = [str(filedataframe.loc[i,'SN'])]
                    cur.execute(sql2,values1)
        valueslist = []
        if (testtimevalue != []):
            if csv_testtimestamp < kudu_testtimestamp:
                valueslist.append(0)
            else:
                valueslist.append(1)
        else:
            valueslist.append(1)
        for item in fileheader:
            valueslist.append(str(filedataframe.loc[i,item]).replace('nan',''))
        valueslist.append(time1_str)
        valueslisttemp = str(valueslist).replace('[','(').replace(']',')')
        if i==0:
            valueslist_str =  valueslisttemp
        else:
            valueslist_str += ',' + valueslisttemp
    if valueslist_str !='':
        try:
            sql = "insert into "+ tablename + columnitems +" values" + valueslist_str
            cur.execute(sql)
            sourcefile = file_folder + os.sep + filename
            destifile = backuppath+os.sep+filename
            moveFile(sourcefile,destifile)
        except Exception,e:
            print e
            sourcefile = file_folder + os.sep + filename
            destifile = errorfolder+os.sep+now_date+os.sep+filename
            moveFile(sourcefile,destifile)
        finally:
            close_Connection(conn)
    else:
        close_Connection(conn)

def handle(func,func_filelist):
    header_dict = headermapping(func)
    for filename in func_filelist:
        OkFile_tag = 'True'
        filepath = file_folder + os.sep + filename
        print filename
        if os.path.exists(filepath):
            if os.path.getsize(filepath) != 0:
                filecreatetime_stamp = datetime.datetime.fromtimestamp(os.stat(filepath).st_ctime)
                filecreatetime_str = datetime.datetime.strftime(filecreatetime_stamp,'%Y%m%d %H:%M:%S')
                filecreatetime = datetime.datetime.strptime(filecreatetime_str,'%Y%m%d %H:%M:%S')
                if filecreatetime < datetime.datetime.now() - datetime.timedelta(seconds=30):
                    filedataframe,fileheader = dataframeInstance(filename)
                    if fileheader == []:
                        continue
                    if ('SN' in fileheader) or ('USN' in fileheader):
                    #fileheader = [item.strip() for item in filedataframe.columns.values]
                        fileindex = filedataframe.index.values
                        tablename = 'allie.kckf_'+func.lower() #allie.kckf_kckf
                        if set(fileheader).issubset(set(header_dict.keys())):
                            dataUpload(fileindex,fileheader,header_dict,tablename,filedataframe,filename)
                        else:
                            itemlist = []
                            for item in fileheader:
                                if item in header_dict.keys():
                                    itemlist.append(item)
                            dataUpload(fileindex,itemlist,header_dict,tablename,filedataframe,filename)
                    else:
                        sourcefile = filepath
                        destifile = errorfolder+os.sep+now_date+os.sep+filename
                        moveFile(sourcefile,destifile)
            else:
                os.remove(filepath)
'''
def removetempfolder(folderpath,backuppath):
    os.rmdir(folderpath)
    delbakuplist = os.listdir(backuppath)
    for f in delbakuplist:
        filePath = os.path.join(backuppath, f)
        os.remove(filePath)
    os.rmdir(backuppath)
'''
if __name__ == "__main__":
    pid = writePid()
    file_folder = '/data/buffer/kudu_kckf_csv_bail_tmp/'+str(pid_id)
    backupfolder = '/data/buffer/hbase_bali_kckf_csv'
    errorfolder ='/data/history/csv/kckf_csv/err_kckf_csv_kudu'
    #file_folder = 'd:\p8_kckf'
    #backupfolder = 'd:\p8_kckf_backup'
    #errorfolder ='d:\p8_kckf_error'
    hostname = 'p8cdhdatap04.wzs.wistron'
    port = 21050
    now_time = datetime.datetime.now()
    now_date = now_time.strftime('%Y%m%d')
    backuppath = backupfolder + os.sep
    try:
        #if not os.path.exists(backuppath):
        #    os.mkdir(backuppath)
        kn_filelist,kckf_filelist,pr_filelist, kckf_r_filelist,isakf_filelist = get_FileList(file_folder)
        if kckf_filelist !=[]:
            func = 'KCKF'
            func_filelist = kckf_filelist
            handle(func,func_filelist)
        if kn_filelist !=[]:
            func = 'KN'
            func_filelist = kn_filelist
            handle(func,func_filelist)
        if pr_filelist !=[]:
            func = 'PR'
            func_filelist = pr_filelist
            handle(func,func_filelist)
        if isakf_filelist !=[]:
            func = 'ISAKF'
            func_filelist = isakf_filelist
            handle(func,func_filelist)
    except Exception,e:
        print e
    finally:
        delPid(pid)
