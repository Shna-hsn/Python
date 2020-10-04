import os,time,happybase,datetime,shutil
from hdfs import InsecureClient
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

now_time = datetime.datetime.now()
rec_dat = [f for f in os.listdir('/hdata/history/jpeg/') if 'lost' not in f]

hdfs_log = ''
hdfs_path = '/P8AOI/MapData/'
client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
modellist = [ model for model in client.list(hdfs_path) if 'X1' in model or 'X3' in model or 'X5' in model or 'R1' in model]


for x in range(len(rec_dat)):
#for x in range(5):
  print rec_dat[x]
  split_path = '/bfdata/history/jpeg/'+rec_dat[x]
  lost_path = '/bfdata/history/jpeg/lost/'
  aoi_file = [ aoi for aoi in os.listdir(split_path) if rec_dat[x] in aoi ]

  for y in range(len(modellist)):
    os.system('/home/hadoop/wistron-hadoop/hadoop-2.7.1/bin/hdfs fsck /P8AOI/MapData/'+ modellist[y] + '/' + rec_dat[x] + ' -files -blocks > /home/hadoop/program/compare_sn/hdfs_log/hdfs_'+  modellist[y] + '_' + rec_dat[x] + '.log')
    hdfs_log1 = open('/home/hadoop/program/compare_sn/hdfs_log/hdfs_'+  modellist[y] + '_' + rec_dat[x] + '.log').read()
    hdfs_log = hdfs_log + hdfs_log1

  for i in range(len(aoi_file)):
    if aoi_file[i] not in hdfs_log:
        print aoi_file[i]
        
        shutil.move(split_path+"/"+aoi_file[i],lost_path+aoi_file[i])
  os.rename('/bfdata/history/jpeg/'+rec_dat[x],'/bfdata/history/jpeg/'+rec_dat[x]+'_checked')

