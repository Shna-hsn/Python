import os,datetime,time,shutil
import random

start_time = time.time()
now_time    = datetime.datetime.now()
rec_dat     = now_time.strftime('%Y%m%d')
if __name__ == '__main__':
    bali_csv = r''
    print(bali_csv)
    if bali_csv.count("_") > 4:
        csv_file_name_pre = bali_csv.split("_",1)[1]
        print(csv_file_name_pre)
        if len(csv_file_name_pre) > 41:
            print(bali_csv.split("_")[-2])

            SN  = bali_csv.split("_")[-2].split("-")[-1]
            print(SN)
            if len(SN) > 16:
                foldernum = str(random.randint(1,5))
                # shutil.move(bali_csv_path+bali_csv,tmp_kckf_csv_path+foldernum+os.sep+bali_csv)
            else:
                filetype = 'Error_bali_kckf_csv'
        else:
                filetype = 'Error_bali_kckf_csv'
    else:
        filetype = 'Error_bali_kckf_csv'

end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
