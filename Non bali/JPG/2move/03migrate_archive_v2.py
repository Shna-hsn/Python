import os,shutil,time,datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

start_time = time.time()
path   = '/data/buffer/total_pre_arch/'
#test for ceph upload
#path   = '/data/buffer/total_pre_arch_ceph/'
arch   = '/data/history/jpeg/'
jpg    = [jpg for jpg in os.listdir(path)]
folder = [folder for folder in os.listdir(arch)]
time.sleep(3)
n = 0
if len(jpg) > 300000:
  n = 300000
else:
  n = len(jpg)

for i in range(n):
    print i,"/",n,"moving",jpg[i]
    if jpg[i].count("@") == 1:
        dt = jpg[i].split("@")[1][:8]
    else:
        dt = jpg[i].split("_")[-2][:8]
    if dt not in folder:
    #    print dt,folder
        os.mkdir(arch+str(dt))
        folder = [folder for folder in os.listdir(arch)]
    shutil.move(path+jpg[i], arch+str(dt)+os.sep+jpg[i])
 
end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
print "Job: 03migrate_archive"
print "Total files count: ",n
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",n / (end_time - start_time),"pcs/s,at date/time: ",com_dat
