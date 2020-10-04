import os,datetime,time,shutil
import random

start_time = time.time()
if __name__ == '__main__':
#    err_jpg_path = '/data/buffer/err_jpg/'
    #err_jpg_path = '/data/buffer/debug_total_jpg/lost/X3A-HT-XY-LEGEND/'
    #err_jpg_path = '/data/buffer/debug_total_jpg/lost/X3A-HT-XY-LEGEND_ok/'
        err_jpg_path = '/data/history/jpeg/lost/'
        #err_jpg_path = '/data/buffer/hight_priority_jpg/'
        total_jpg_path = '/hdata/buffer/debug_total_jpg'
#    total_jpg_path = '/data/buffer/debug_total_jpg/lost'
#    while 1:
        errfile_list = [a for a in os.listdir(err_jpg_path) if '.JPG' in a]
        time.sleep(5)
        errlist_len = 0
        if len(errfile_list) > 40000:
            errlist_len = 40000
            n = errlist_len
        else:
            errlist_len = len(errfile_list)
            n = errlist_len
        for i in range(errlist_len):
            err_jpg = errfile_list[i]
            #ok_jpg = err_jpg.replace(' ','')
            ok_jpg = err_jpg
            foldernum = str(random.randint(1,6)) + '00'
            shutil.move(err_jpg_path+err_jpg,total_jpg_path+os.sep+foldernum+os.sep+ok_jpg)
            print i,"/",n,"moving",err_jpg
end_time = time.time()
end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
print "Job: 01jpg_lost_jpg_move_to_debug_total_jpg_today"
print "Total files count: ",n
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",n / (end_time - start_time),"pcs/s,at date/time: ",com_dat
