import os,datetime,time,happybase,shutil
from hdfs import InsecureClient
from impala.dbapi import connect
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def rename(aoi_jpg):
    new_name = aoi_jpg.replace(' ','')
    return new_name

def writeHBase(aoi_file):
    connection = happybase.Connection('10.41.158.65')
    table = connection.table('ie4_p8_aoi')
    temp = {}
    #sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")
    sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")+"_"+str(aoi_file.split("@")[1].split("-",1)[0][:12])
    if len(sn)> 10:
        dt = str(aoi_file.split("@")[1].split("-",1)[0][:8])
        cf = str(aoi_file.split("@")[1].split("-",1)[0].replace(' ',''))
        aoi_file = rename(aoi_file)
        value = hdfs_path + dt + os.sep + aoi_file
        temp["cf:"+str(cf)] = value
        table.put(sn,temp)
        print "writeHBase ok"


def writeKudu(aoi_file):
    tablename ="allie.aoi_imageurl"
    columnitems = "(usn,trndate,imageurl)"
    hostname1 = 'p8cdhdatap06.wzs.wistron'
    hostname2 = 'p8cdhdatap07.wzs.wistron'
    hostname3 = 'p8cdhdatap04.wzs.wistron'
    port = 21050
    aoi_file.decode('string_escape')
    #sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")
    sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")+"_"+str(aoi_file.split("@")[1].split("-",1)[0][:12])
    aoi_file = rename(aoi_file)
    if len(sn) > 10:
        if '@D@' in aoi_file:
            dt = aoi_file[aoi_file.find('@')+3:aoi_file.find('@')+11]
        elif '@' in aoi_file:
            dt = aoi_file[aoi_file.find('@')+1:aoi_file.find('@')+9]
        value = hdfs_path + dt + '/' + aoi_file
        date_stamp = datetime.datetime.now()
        date_stamp_str = datetime.datetime.strftime(date_stamp,'%Y-%m-%d %H:%M:%S')
        valuelist =[str(sn),date_stamp_str,str(value)]
        valuelist = str(valuelist).replace('[','(').replace(']',')')
        sql = "insert into "+ tablename + columnitems +" values" + valuelist
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
                print "writeKudu ok"
            except Exception,e:
                print e
            conn.close()

def uploadHDFS(filename):
#    if ' ' in filename:
#        aoi_file = rename(aoi_file)
#    else:
    aoi_file = filename
    client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
    fname1 = client.list(hdfs_path)
#    if aoi_file.split("@").count('-') >= 2:
    dt = str(aoi_file.split("@")[1].split("-",1)[0][:8])
#    else:
#        dt = str(aoi_file.split("_")[-1].split("-")[-2].split("@")[1][:8])
#        dt = str(aoi_file.split("@")[1].split("-",1)[0][:8])
    folder1 = dt
    if folder1 in fname1:
        client.upload(hdfs_path+folder1+"/"+aoi_file,upload_path+aoi_file,overwrite=True)
        print "uploadHDFS ok"
        shutil.move(upload_path + aoi_file, backup_path + aoi_file)
    else:
        client.makedirs(hdfs_path+folder1)
        client.upload(hdfs_path+folder1+"/"+aoi_file,upload_path+aoi_file,overwrite=True)
        print "uploadHDFS ok"
        shutil.move(upload_path + aoi_file, backup_path + aoi_file)

def handle():
    #aoi_file = [ aoi for aoi in os.listdir(upload_path) if rec_dat not in aoi ]
    aoi_file = [ aoi for aoi in os.listdir(upload_path)]
    if len(aoi_file) > 1000:
        n = 1000
    else:
        n = len(aoi_file)
    for i in range(n):
        print aoi_file[i]
        if aoi_file[i].startswith('192.'):
            shutil.move(upload_path + aoi_file[i], bali_path + aoi_file[i])
        else:
            try:
                writeHBase(aoi_file[i])
            except Exception,e:
                print e
            try:
                writeKudu(aoi_file[i])
            except Exception,e:
                print e
            newname = rename(aoi_file[i])
            os.rename(upload_path + aoi_file[i],upload_path + newname)
            uploadHDFS(newname)
          # shutil.move(upload_path + newname, backup_path)
    #uploadHDFS(aoi_file[i])

if __name__ == "__main__":
    rec_dat = datetime.datetime.now().strftime('%Y%m%d')
    upload_path = '/bfdata/buffer/total_jpg_carrier/'
    backup_path = '/hdata/buffer/total_pre_arch/'
    hdfs_path   = '/P8AOI/MapData/'
    bali_path  = '/bfdata/sftp/aoisftp/upload_bali/'
    start_time = time.time()
    print "Start time:",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    handle()
