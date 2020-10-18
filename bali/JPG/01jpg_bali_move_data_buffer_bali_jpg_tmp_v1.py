import os,datetime,time,shutil
import random

start_time = time.time()
if __name__ == '__main__':
#    err_jpg_path = '/data/buffer/err_jpg/'
    #err_jpg_path = '/data/buffer/debug_total_jpg/lost/X3A-HT-XY-LEGEND/'
    #err_jpg_path = '/data/buffer/debug_total_jpg/lost/X3A-HT-XY-LEGEND_ok/'
        bali_jpg_path = '/data/sftp/aoisftp/upload_bali/'
        #err_jpg_path = '/data/buffer/hight_priority_jpg/'
        tmp_jpg_path = '/data/buffer/bali_aoi_jpg_tmp'
#    total_jpg_path = '/data/buffer/debug_total_jpg/lost'
#    while 1:
        bali_file_list = [a for a in os.listdir(bali_jpg_path) if '.JPG' in a]
        time.sleep(10)
        bali_list_len = 0
        if len(bali_file_list) > 10000:
            bali_list_len = 10000
            n = bali_list_len
        else:
            bali_list_len = len(bali_file_list)
            n = bali_list_len
        for i in range(bali_list_len):
            bali_jpg = bali_file_list[i]
            #ok_jpg = err_jpg.replace(' ','')
            ok_jpg = bali_jpg
            foldernum = str(random.randint(1,5))
            shutil.move(bali_jpg_path+bali_jpg,tmp_jpg_path+os.sep+foldernum+os.sep+ok_jpg)
            print i,"/",n,"moving",bali_jpg
end_time = time.time()
end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
print "Job: 01jpg_"
print "Total files count: ",n
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",n / (end_time - start_time),"pcs/s,at date/time: ",com_dat
