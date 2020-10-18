import os,time,happybase,datetime,shutil,random
from hdfs import InsecureClient
from impala.dbapi import connect
from collections import defaultdict
import cx_Oracle
import sys

## check total_tmp fold empty, clear tmpfile
def moveTempfile_To_fix_path():
    aoi_tmp_model = os.listdir(temp_path)
    for i in range(len(aoi_tmp_model)):
        aoi_tmp_file = os.listdir(temp_path + aoi_tmp_model[i])
        if len(aoi_tmp_file) > 0:
            print 'total_tmp is not empty,path have ', len(aoi_tmp_file),'files.'
            print 'mv -f ',temp_path + aoi_tmp_model[i]+'/'+"*",fix_path
            os.system('mv -f ' + (temp_path + aoi_tmp_model[i]) + '/'+"* " + fix_path)
            # time.sleep(120)

def rename(aoi_jpg):
    new_name = aoi_jpg.replace(' ','')
    return new_name

def getfilesn(aoi_file):
    filesn = aoi_file.split('@')[-1].split('.')[0].split('-')[-1]
    return filesn

def getusnlist(aoi_file):
    listusn=''
    if len(aoi_file) > 8000:
        n = 8000
    else:
        n = len(aoi_file)
    for i in range(n):        
        if aoi_file[i].startswith('192.'):
            shutil.move(upload_path + aoi_file[i], bali_path + aoi_file[i])
        else:
            newname = rename(aoi_file[i])
            sn=getfilesn(aoi_file[i])
            if i == 0:
                listusn = '('+ '1' + ',' + '\''+sn+'\''+')'
            else:
                listusn = listusn +','+'('+ '1' + ',' +'\''+sn+'\''+ ')'
    return listusn

def getusnmodel(usn):
    # conn=cx_Oracle.connect('SFCFA139/SFCFA139@10.41.129.33/orcl')
    result=''
    sql="select usn,model from sfcmodel a,sfcusn b where a.upn=b.upn and (1,b.usn) in ("+usn+")  union all "+"select usn,model from sfcpcb139.sfcmodel a, sfcpcb139.sfcusn b where a.upn=b.upn and (1,b.usn) in ("+usn+") "
    try:
        conn=cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
        print('conn OK')
        curs=conn.cursor()
        rr=curs.execute(sql)
        result=curs.fetchall()

        curs.close()
        conn.close()
        return result
    except Exception as e:
        print e
        print 'conn FAiL'
        model='Errormodel'
    print time.time()-start_time
    return result

def writeHBase(aoi_file,usn,model):
    connection = happybase.Connection('10.41.158.65')
    table = connection.table('ie4_p8_aoi')
    temp = {}
    #sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")
    #model=dir[sn][0]
    print "HBASE:",aoi_file
    if len(usn)> 10:
        dt = str(aoi_file.split("@")[1].split("-",1)[0][:8])
        cf = str(aoi_file.split("@")[1].split("-",1)[0].replace(' ',''))
        aoi_file = rename(aoi_file)
        value = hdfs_path +model+'/'+ dt + '/' + aoi_file
        temp["cf:"+str(cf)] = value
        table.put(usn,temp)


def writeKudu(kudu_filelist,dir):
    tablename ="allie.aoi_imageurl"
    columnitems = "(usn,trndate,imageurl)"
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'
    port = 21050
    valuelist_str = ' '
    print "kudu:",kudu_filelist
    for i in range(len(kudu_filelist)):
        aoi_file=kudu_filelist[i]       
        #aoi_file.decode('string_escape')
        sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")
        if dir.get(sn,"Errormodel") == 'Errormodel':
            model = 'Errormodel'
        else:
            model=dir[sn][0]
        aoi_file = rename(aoi_file)
        if len(sn) > 10:
            if '@D@' in aoi_file:
                dt = aoi_file[aoi_file.find('@')+3:aoi_file.find('@')+11]
            elif '@' in aoi_file:
                dt = aoi_file[aoi_file.find('@')+1:aoi_file.find('@')+9]
            value = hdfs_path + model +'/'+ dt + '/' + aoi_file
            date_stamp = datetime.datetime.now()
            date_stamp_str = datetime.datetime.strftime(date_stamp,'%Y-%m-%d %H:%M:%S')
            valuelist =[str(sn),date_stamp_str,str(value)]
            valuelist = str(valuelist).replace('[','(').replace(']',')')
            #sql = "insert into "+ tablename + columnitems +" values" + valuelist
        if i == 0:
            valuelist_str = valuelist
        else:
            valuelist_str =  valuelist_str + ',' + valuelist

    sql = "insert into "+tablename+columnitems+" values"+valuelist_str
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
            #print "writeKudu ok"
        except Exception as e:
            print e
        conn.close()

def uploadHDFS():
    client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
    fname1 = client.list(hdfs_path)
    #print fname1
    modelfile = os.listdir(temp_path)
    print modelfile
    for i in range(len(modelfile)):
        if modelfile[i] not in fname1:
            client.makedirs(hdfs_path+modelfile[i])
        fname2 = client.list(hdfs_path+modelfile[i]+'/')
        if rec_dat not in fname2:
            client.makedirs(hdfs_path+modelfile[i]+"/"+rec_dat)
        files = os.listdir(temp_path + modelfile[i] + '/')
        if len(files) > 0:
            src = temp_path + modelfile[i] + '/' + '*'
            backup_path = '/bfdata/buffer/total_pre_arch/'
            dsc = hdfs_path + modelfile[i] + '/' + rec_dat + '/'
            print 'hdfs dfs -copyFromLocal ',src,dsc
            os.system('/home/hadoop/wistron-hadoop/hadoop-2.7.1/bin/hdfs dfs -copyFromLocal ' + src + ' ' + dsc)
            print 'mv -f ',src ,backup_path 
            os.system('mv -f ' + src + ' ' + backup_path )
            end_time = time.time()
            com_dat = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')


def handle():
    moveTempfile_To_fix_path()
    aoi_file = [ aoi for aoi in os.listdir(upload_path) if rec_dat in aoi]
    dir = defaultdict(list)
    listusn = getusnlist(aoi_file)
    #print listusn
    result = getusnmodel(listusn)
    for x in range(len(result)):
        dir[result[x][0]].append(result[x][1])
    #print dir 
    if len(aoi_file) > 0:
        if len(aoi_file) < 8000:
            aoi_count = len(aoi_file)
        else:
            aoi_count = 8000
        print "The total amount of this upload is:",aoi_count
        kudu_count = 0
        kudu_filelist = []
        for i in range(aoi_count):
            newname = rename(aoi_file[i])
            os.rename(upload_path + aoi_file[i],upload_path + newname)
            usn = getfilesn(aoi_file[i])
            if dir.get(usn,"Errormodel") == 'Errormodel':
                model = 'Errormodel'
            else:
                model = dir[usn][0]
            #print usn,model
            modelfile = os.listdir(temp_path)
            if model not in modelfile:
                os.makedirs(temp_path +  model)
            shutil.move(upload_path + newname, temp_path +  model)
            try:
                writeHBase(aoi_file[i],usn,model)
            except Exception as e:
                print e
            try:
                kudu_count += 1
                kudu_filelist.append(aoi_file[i])
                if(kudu_count>=800) or (i==(aoi_count-1)):
                    print "kudu_count:",kudu_count
                    writeKudu(kudu_filelist,dir)
                    kudu_count = 0
                    kudu_filelist = []
            except Exception as e:
                print e
                
    uploadHDFS()


if __name__ == "__main__":
    start_time = time.time()
    print "Start time:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    now_time = datetime.datetime.now()
    rec_dat = now_time.strftime('%Y%m%d')
    upload_path = '/bfdata/buffer/total_jpg/'
    temp_path = '/bfdata/buffer/total_tmp04/'
    hdfs_path = '/P8AOI/MapData/'
    fix_path = '/bfdata/buffer/debug_total_jpg/today/'
    bali_path  = '/bfdata/sftp/aoisftp/upload_bali/'
    handle()
    print "End time:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())