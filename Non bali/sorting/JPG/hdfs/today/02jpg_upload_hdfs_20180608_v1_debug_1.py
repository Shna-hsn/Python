import os,time,happybase,datetime,shutil,random
from hdfs import InsecureClient
from impala.dbapi import connect
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

## check total_tmp fold empty, clear tmpfile
def moveTempfile_To_fix_path():
    aoi_tmp_file = os.listdir(temp_path)
    if len(aoi_tmp_file) > 0:
        print 'total_tmp is not empty,path have ', len(aoi_tmp_file),'files.'
        print 'mv -f ',temp_path+"*",fix_path
        os.system('mv -f ' + temp_path + "* " + fix_path)
        time.sleep(120)

## drop the space
def rename(aoi_jpg):
    new_name = aoi_jpg.replace(' ','')
    #new_name = aoi_jpg.replace(' ','').split("_")
    #func = new_name_pre[0].replace(' ','')
    #host = new_name_pre[-1].split("@")[0]
    #ctime = new_name_pre[-1].split("@")[1].split("-")[0]
    #dt = new_name_pre[-1].split("@")[1].split("-")[0][:8]
    #sn = new_name_pre[-1].split("@")[1].split("-",1)[1].replace(".JPG","")
    #new_name = func + "_" + dt + "_" + host + "@" + ctime +"-"+ sn + ".JPG"
    return new_name

## For HBase connection
def writeHBase(aoi_file):
    connection = happybase.Connection('10.41.158.65')
    table = connection.table('ie4_p8_aoi')
    temp = {}
    sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")
    if len(sn)> 10:
        dt = str(aoi_file.split("@")[1].split("-",1)[0][:8])
        cf = str(aoi_file.split("@")[1].split("-",1)[0].replace(' ',''))
#        print aoi_file
        aoi_file = rename(aoi_file)
#        print 'rename'+aoi_file
        value = hdfs_path + dt + os.sep + aoi_file
#        print value
        temp["cf:"+str(cf)] = value
        #print i,"/",n," ",sn,temp
        table.put(sn,temp)

def writeKudu(kudu_filelist):
    tablename ="allie.aoi_imageurl"
    columnitems = "(usn,trndate,imageurl)"
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'
    port = 21050
    valueslist_str =' '
    for i in range(len(kudu_filelist)):
        aoi_file = kudu_filelist[i]
        aoi_file.decode('string_escape')
        sn = aoi_file.split("@")[1].split("-",1)[1].replace(".JPG","")
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
        if i==0:
            valueslist_str = valuelist
        else:
            valueslist_str += ',' + valuelist
    sql = "insert into "+ tablename + columnitems +" values" + valueslist_str
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
           # print 'kudu ok'
        except Exception,e:
            print e
        conn.close()


## Upload file to HDFS
def uploadHDFS():
    client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
    fname1 = client.list(hdfs_path)
    if rec_dat not in fname1:
        client.makedirs(hdfs_path+"/"+rec_dat)
    src = temp_path + '*'
    backup_path = '/bfdata/buffer/total_pre_arch/'
    dsc = hdfs_path + rec_dat + '/'
    print 'hdfs dfs -copyFromLocal ',src,dsc
    os.system('/home/hadoop/wistron-hadoop/hadoop-2.7.1/bin/hdfs dfs -copyFromLocal ' + src + ' ' + dsc)
    print 'mv -f ',temp_path+"*",backup_path
    os.system('mv -f ' + temp_path + "* " + backup_path )
#os.system('/usr/bin/find ' + temp_path + ' -name *.JPG -exec mv {} ' + backup_path + ' \;')
    end_time = time.time()
    com_dat = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
#    print "Job: 02jpg_upload_hdfs"
#    print "Total files count: ",aoi_count
#    print "Total time:      ", end_time - start_time,"s"
#    print "Speed: " / (end_time - start_time),"pcs/s,at date/time: ",com_dat

# 1. move temp before file to fix_path
# 2. get upload_path folder filelist and start  handling
# 3. write hbase
# 4. write Kudu
# 5. rename and move to temp_path
# 6 upload files to HDFS
def handle():
    moveTempfile_To_fix_path()
    aoi_file = [ aoi for aoi in os.listdir(upload_path) if rec_dat in aoi]
    if len(aoi_file) > 0:
        if len(aoi_file) < 14000:
            aoi_count = len(aoi_file)
        else:
            aoi_count = 14000
        kudu_count = 0
        kudu_filelist = []
        for i in range(aoi_count):
            try:
                writeHBase(aoi_file[i])
            except Exception,e:
                print e
            try:
                kudu_count +=1
                kudu_filelist.append(aoi_file[i])
                if (kudu_count >=1000) or (i==(aoi_count-1)):
                    writeKudu(kudu_filelist)
                    kudu_count = 0
                    kudu_filelist = []
            except Exception,e:
                print e
            newname = rename(aoi_file[i])
            os.rename(upload_path + aoi_file[i],upload_path + newname)
            shutil.move(upload_path + newname, temp_path)
    uploadHDFS()

if __name__ == "__main__":
    start_time = time.time()
    now_time = datetime.datetime.now()
    rec_dat = now_time.strftime('%Y%m%d')
    upload_path = '/bfdata/buffer/total_jpg/'
    temp_path = '/bfdata/buffer/total_tmp_debug_01/'
    hdfs_path = '/P8AOI/MapData/'
    fix_path = '/bfdata/buffer/debug_total_jpg/today/'
    handle()


