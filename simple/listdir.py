import os

path = r'/data/buffer/hbase_aoi_csv/'
fileslist  = os.listdir(path)
for filename in fileslist:
  print filename