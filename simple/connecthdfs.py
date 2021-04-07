from hdfs import InsecureClient

client = InsecureClient('http://10.41.158.65:50070', user='hadoop')
fname = client.list('/P8AOI/MapData/X1724/20210401')
print(fname)