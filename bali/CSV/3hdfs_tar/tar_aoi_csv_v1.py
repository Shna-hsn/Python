import tarfile,os,datetime,shutil
from hdfs import InsecureClient

def tar_csv(path,func):
    tar = tarfile.open(path+yes_date+"_"+func+"_csv.tar.gz","w:gz")
    for root,dir,files in os.walk(path+yes_date):
      for file in files:
        fullpath = os.path.join(root,file)
        tar.add(fullpath)
    tar.close()
    print "tar zip complete"
    print "uploading to hdfs"
    client.upload(hdfs+yes_date+"_"+func+"_csv.tar.gz",path+yes_date+"_"+func+"_csv.tar.gz",overwrite=True)
    print "upload hdfs copmlete"
    os.system('rm -rf '+path+yes_date)
    shutil.move(path+yes_date+"_"+func+"_csv.tar.gz",history_path+func+'_csv'+os.sep) 
    return

yes_time = datetime.datetime.now() + datetime.timedelta(days=-4)
yes_date = yes_time.strftime('%Y%m%d')
base_aoi = '/data/history/csv/aoi_csv/'
tar_aoi_path = base_aoi+yes_date
base_kckf = '/data/history/csv/kckf_csv/'
tar_kckf_path = base_kckf+yes_date
history_path = '/hdata/history/csv/'
hdfs = '/P8AOI/csv_archive/'
client = InsecureClient('http://10.41.158.65:50070', user='hadoop')

print yes_date
if os.path.exists(tar_aoi_path):
    print tar_aoi_path
    aoi_csv_file = os.listdir(tar_aoi_path)
    count_aoi_csv = len(aoi_csv_file)
    print "count aoi csv =",str(count_aoi_csv)
    if count_aoi_csv > 1:
        tar_csv(base_aoi, 'aoi' )
if os.path.exists(tar_kckf_path):
    print tar_kckf_path
    kckf_csv_file = os.listdir(tar_kckf_path)
    count_kckf_csv = len(kckf_csv_file)
    print "count kckf csv =",str(count_kckf_csv)
    kckf_csv_file = os.listdir(tar_kckf_path)
    if count_kckf_csv > 1:
        tar_csv(base_kckf,'kckf')


#tar = tarfile.open(base+yes_date+"_aoi_csv.tar.gz","w:gz")

#for root,dir,files in os.walk(base+yes_date):
#  for file in files:
#    fullpath = os.path.join(root,file)
#    tar.add(fullpath)
#tar.close()

#client.upload(hdfs+yes_date+"_aoi_csv.tar.gz",base+yes_date+"_aoi_csv.tar.gz",overwrite=True)

#os.system('rm -rf '+base+yes_date)

## For KCKF csv upload
#base_kckf = '/kckf_archive/kckf_csv/'
#hdfs = '/P8AOI/csv_archive/'

#tar = tarfile.open(base_kckf+yes_date+"_kckf_csv.tar.gz","w:gz")
#
#for root,dir,files in os.walk(base_kckf+yes_date):
#  for file in files:
#    fullpath = os.path.join(root,file)
#    tar.add(fullpath)
#tar.close()

#client.upload(hdfs+yes_date+"_kckf_csv.tar.gz",base_kckf+yes_date+"_kckf_csv.tar.gz",overwrite=True)

#os.system('rm -rf '+base_kckf+yes_date)

