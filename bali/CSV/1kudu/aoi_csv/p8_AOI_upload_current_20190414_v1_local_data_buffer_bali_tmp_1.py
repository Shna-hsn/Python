# -*- coding:utf8 -*-
import os,csv,shutil,datetime,time
import pandas as pd
import tarfile
from impala.dbapi import connect


def writePid():
    pid = str(os.getpid())
    f = open(pid_folder+os.sep+'AOIPID_local_buffer_tmp_'+str(pid_id)+'.pid', 'a+')
    f.write(pid+'\n')
    f.close()
    return pid

def readPidCount():
    with open(pid_folder+os.sep+'AOIPID_local_buffer_tmp_'+str(pid_id)+'.pid', 'r') as f:
        lines = f.readlines()
        count = len(lines)
    return count

def delPid(pid):
    with open(pid_folder+os.sep+'AOIPID_local_buffer_tmp_'+str(pid_id)+'.pid', 'r') as f:
        lines = f.readlines()
    with open(pid_folder+os.sep+'AOIPID_local_buffer_tmp_'+str(pid_id)+'.pid', 'w') as w_f:
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
    kb_filelist =[]
    kb_r_filelist =[]
    bf_filelist=[]
    bf_r_filelist = []
    psa_filelist =[]
    psa_r_filelist =[]
    ahs_filelist = []
    ahs_r_filelist = []
    ht_filelist = []
    ht_r_filelist = []
    wf_filelist = []
    wf_r_filelist = []
    uic_filelist = []
    uic_r_filelist = []
    hs_filelist = []
    hs_r_filelist = []
    fp_filelist = []
    emr_filelist = []
    co_filelist = []
    co_r_filelist = []
    mr_filelist = []
    kf_filelist = []
    muk_filelist = []
    muk_psa_filelist = []
    isakf_filelist = []
    test_buck_filelist = []
    shim_filelist = []
    pidcount = readPidCount()
    if pidcount == 1:
        fileslist = os.listdir(folderpath)
    else:
        fileslist = []
        fileslisttemp = os.listdir(folderpath)
        for filename in fileslisttemp:
            filepath = file_folder + os.sep + filename
            filecreatetime_stamp = datetime.datetime.fromtimestamp(os.stat(filepath).st_ctime)
            filecreatetime_str = datetime.datetime.strftime(filecreatetime_stamp,'%Y%m%d %H:%M:%S')
            filecreatetime = datetime.datetime.strptime(filecreatetime_str,'%Y%m%d %H:%M:%S')
            if filecreatetime > datetime.datetime.now() - datetime.timedelta(minutes=2):
                fileslist.append(filename)
    errorfolderdt = errorfolder + os.sep + now_date
    if not os.path.exists(errorfolderdt):
        os.mkdir(errorfolderdt)
    for i in range(len(fileslist)):
        if ('3DVoidCoverArea_PG-BACKE' in fileslist[i]) or('NewHS_PG-BACK' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kb_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep + fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('3DVoidCoverArea_PG' in fileslist[i]) or ('NewHS_PG' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kb_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_AOI7_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kb_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('ISA_PG' in fileslist[i]) and ('NewISA' not in fileslist[i]) and ('ISA_PG-BACK' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                bf_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_AOI3_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                bf_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('ISA_PG-BACK' in fileslist[i]) and ('NewISA' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                bf_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('ShiftRotate_PG' in fileslist[i]) and ('ShiftRotate_PG-BACK' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                psa_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_AOI1_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                psa_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'ShiftRotate_PG-BACK' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                psa_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('ISAH_PG' in fileslist[i]) and ('NewISAH' not in fileslist[i]) and ('ISAH_PG-BACK' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                ahs_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('ISAH_PG-BACK' in fileslist[i]) and ('NewISAH' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                ahs_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('XY_HT_PG' in fileslist[i]) and ('XY_HT_PG-BACK' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                ht_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_AOI8_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                ht_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'XY_HT_PG-BACK' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                ht_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('Stiffener' in fileslist[i]) and ('Stiffener_PG' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                wf_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('DisXYZ_PG-BACK' in fileslist[i]) or ('Stiffener_PG-BACK' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                wf_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('NewISA_PG' in fileslist[i]) and ('NewISA_PG-BACK' not in filelist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                uic_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'NewISA_PG-BACK' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                uic_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_AOI4_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                ahs_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('NewISAH_PG' in fileslist[i]) and ('NewISAH_PG-BACK' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                hs_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'NewISAH_PG-BACK' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                hs_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'FP_PG' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                fp_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('EMR_PG' in fileslist[i]) or ('DisXYZ_PG' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                emr_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'IR_PG' in fileslist[i] and ('IR_PG-BACK' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                co_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'IR_PG-BACK' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                co_r_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'NEWFPPSA_PG' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                mr_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'PRE-KEY-FORCE' in fileslist[i] and ('ISAKF' not in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                kf_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'IMUKPSA_PG' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                muk_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_UK1_' in fileslist[i]) or ('_AOI5_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                muk_psa_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'FMUK_PG' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                muk_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('_AOI16_' in fileslist[i]) or ('_AOI6_' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                muk_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'ISAKF' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                isakf_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif 'Stiffener_PG' in fileslist[i]:
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                test_buck_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sep+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
        elif ('Shim3D_PG' in fileslist[i]) or ('ANSI_SHIM' in fileslist[i]):
            if ('~lock' not in fileslist[i]) or ('._' not in fileslist[i]):
                shim_filelist.append(fileslist[i])
            else:
                #shutil.move(folderpath+os.sep+fileslist[i],errorfolderdt+os.sepp+fileslist[i])
                sourcefile = folderpath+os.sep+fileslist[i]
                destifile = errorfolderdt+os.sep + fileslist[i]
                moveFile(sourcefile,destifile)
    return kb_filelist,kb_r_filelist,bf_filelist,bf_r_filelist,psa_filelist,psa_r_filelist,ahs_filelist,ahs_r_filelist,\
ht_filelist,ht_r_filelist,wf_filelist,wf_r_filelist,uic_filelist,uic_r_filelist,hs_filelist,hs_r_filelist,\
 fp_filelist,emr_filelist,co_filelist,co_r_filelist,mr_filelist,kf_filelist,muk_filelist,muk_psa_filelist,isakf_filelist,\
test_buck_filelist,shim_filelist

def impalaConnection(hostname,port):
    hostname1 = 'p8cdhdatap02.wzs.wistron'
    hostname2 = 'p8cdhdatap03.wzs.wistron'
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
            values = [str(filedataframe.loc[i,'SN']),str(filedataframe.loc[i,'Key_Name']),str(filedataframe.loc[i,'Location'])]
            cur.execute(sql1,values)
            testtimevalue = cur.fetchall()
            if (testtimevalue != []):
                if '-' in testtimevalue[0][0]:
                    testtimevaluetemp = testtimevalue[0][0].replace('-','/')
                    kudu_testtimestamp = datetime.datetime.strptime(testtimevaluetemp,'%Y/%m/%d %H:%M:%S')
                else:
                    kudu_testtimestamp = datetime.datetime.strptime(testtimevalue[0][0],'%Y/%m/%d %H:%M:%S')
                test_start_time = filedataframe.loc[i,'Test_Start_Time']
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
        if filedataframe.loc[i,'Test_Start_Time'] <> '':
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
            print filename
            sourcefile = file_folder + os.sep + filename
            destifile = errorfolder+os.sep+now_date+os.sep+filename
            moveFile(sourcefile,destifile)
            #shutil.move(filepath,errorfolder+os.sep+now_date+os.sep+filename)
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
                        fileindex = filedataframe.index.values
                        tablename = 'allie.aoi_'+func.lower() #allie.kckf_kckf
                        if set(fileheader).issubset(set(header_dict.keys())):
                            dataUpload(fileindex,fileheader,header_dict,tablename,filedataframe,filename)
                        else:
                            itemlist = []
                            for item in fileheader:
                                if item in header_dict.keys():
                                    itemlist.append(item)
                            dataUpload(fileindex,itemlist,header_dict,tablename,filedataframe,filename)
                    else:
                        #shutil.move(filepath,errorfolder+os.sep+now_date+os.sep+filename)
                        sourcefile = filepath
                        destifile = errorfolder+os.sep+now_date+os.sep+filename
                        moveFile(sourcefile,destifile)
            else:
                os.remove(filepath)

if __name__ == "__main__":
    try:
        pid_id = 1
        pid_folder = '/home/armap/kuduprogram/p8_AOI_upload_data_buffer_bali_tmp'
        file_folder = '/data/buffer/kudu_aoi_csv_bail_tmp/'+str(pid_id)
        backupfolder = '/data/buffer/hbase_bali_aoi_csv'
        errorfolder ='/data/history/csv/aoi_csv/err_aoi_csv_kudu'
        pid = writePid()
        #file_folder = 'd:\p8_aoi'
        #backupfolder ='d:\p8_aoi_backup'
        #errorfolder = 'd:\p8_aoi_error'
        hostname = 'p8cdhdatap06.wzs.wistron'
        port = 21050
        now_time = datetime.datetime.now()
        now_date = now_time.strftime('%Y%m%d')
        backuppath = backupfolder + os.sep
        #if not os.path.exists(backuppath):
        #    os.mkdir(backuppath)
        kb_filelist,kb_r_filelist,bf_filelist,bf_r_filelist,psa_filelist,psa_r_filelist,ahs_filelist,ahs_r_filelist,\
        ht_filelist,ht_r_filelist,wf_filelist,wf_r_filelist,uic_filelist,uic_r_filelist,hs_filelist,hs_r_filelist,\
        fp_filelist,emr_filelist,co_filelist,co_r_filelist,mr_filelist,kf_filelist,muk_filelist,muk_psa_filelist,\
        isakf_filelist,test_buck_filelist,shim_filelist = get_FileList(file_folder)
        if kb_filelist !=[]:
            func = 'KB'
            func_filelist = kb_filelist
            handle(func,func_filelist)

        if kb_r_filelist !=[]:
            func = 'KB_R'
            func_filelist = kb_r_filelist
            handle(func,func_filelist)

        if bf_filelist !=[]:
            func = 'BF'
            func_filelist = bf_filelist
            handle(func,func_filelist)

        if bf_r_filelist !=[]:
            func = 'BF_R'
            func_filelist = bf_r_filelist
            handle(func,func_filelist)

        if psa_filelist !=[]:
            func = 'PSA'
            func_filelist = psa_filelist
            handle(func,func_filelist)

        if psa_r_filelist !=[]:
            func = 'PSA_R'
            func_filelist = psa_r_filelist
            handle(func,func_filelist)

        if ahs_filelist !=[]:
            func = 'AHS'
            func_filelist = ahs_filelist
            handle(func,func_filelist)

        if ahs_r_filelist !=[]:
            func = 'AHS_R'
            func_filelist = ahs_r_filelist
            handle(func,func_filelist)
        #if func_list[i] == ht_filelist:
        if ht_filelist !=[]:
            func = 'HT'
            func_filelist = ht_filelist
            handle(func,func_filelist)
        #if func_list[i] == ht_r_filelist:
        if ht_r_filelist !=[]:
            func = 'HT_R'
            func_filelist = ht_r_filelist
            handle(func,func_filelist)
        #if func_list[i] == wf_filelist:
        if wf_filelist !=[]:
            func = 'WF'
            func_filelist = wf_filelist
            handle(func,func_filelist)
        #if func_list[i] == wf_r_filelist:
        if wf_r_filelist !=[]:
            func = 'WF_R'
            func_filelist = wf_r_filelist
            handle(func,func_filelist)
        #if func_list[i] == uic_filelist:
        if uic_filelist !=[]:
            func = 'UIC'
            func_filelist = uic_filelist
            handle(func,func_filelist)
        #if func_list[i] == uic_r_filelist:
        if uic_r_filelist !=[]:
            func = 'UIC_R'
            func_filelist = uic_r_filelist
            handle(func,func_filelist)
        #if func_list[i] == hs_filelist:
        if hs_filelist !=[]:
            func = 'HS'
            func_filelist = hs_filelist
            handle(func,func_filelist)
        #if func_list[i] == hs_r_filelist:
        if hs_r_filelist !=[]:
            func = 'HS_R'
            func_filelist = hs_r_filelist
            handle(func,func_filelist)
        #if func_list[i] == fp_filelist:
        if fp_filelist !=[]:
            func = 'FP'
            func_filelist = fp_filelist
            handle(func,func_filelist)
        #if func_list[i] == emr_filelist:
        if emr_filelist !=[]:
            func = 'EMR'
            func_filelist = emr_filelist
            handle(func,func_filelist)
        #if func_list[i] == co_filelist:
        if co_filelist !=[]:
            func = 'CO'
            func_filelist = co_filelist
            handle(func,func_filelist)
        #if func_list[i] == co_r_filelist:
        if co_r_filelist !=[]:
            func = 'CO_R'
            func_filelist = co_r_filelist
            handle(func,func_filelist)
        #if func_list[i] == mr_filelist:
        if mr_filelist !=[]:
            func = 'MR'
            func_filelist = mr_filelist
            handle(func,func_filelist)
        #if func_list[i] == kf_filelist:
        if kf_filelist !=[]:
            func = 'KF'
            func_filelist = kf_filelist
            handle(func,func_filelist)
        #if func_list[i] == muk_filelist:
        if muk_filelist !=[]:
            func = 'MUK'
            func_filelist = muk_filelist
            handle(func,func_filelist)
        #if func_list[i] == muk_psa_filelist:
        if muk_psa_filelist !=[]:
            func = 'MUK_PSA'
            func_filelist = muk_psa_filelist
            handle(func,func_filelist)
        if isakf_filelist != []:
            func = 'ISAKF'
            func_filelist = isakf_filelist
            handle(func,func_filelist)
        if test_buck_filelist != []:
            func = 'TEST_BUCK'
            func_filelist = test_buck_filelist
            handle(func,func_filelist)
        if shim_filelist != []:
            func = 'SHIM'
            func_filelist = shim_filelist
            handle(func,func_filelist)
    except Exception,e:
        print e
    finally:
        delPid(pid)
