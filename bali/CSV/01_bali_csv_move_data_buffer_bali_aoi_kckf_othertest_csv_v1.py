import os,datetime,time,shutil
import random

start_time = time.time()
now_time    = datetime.datetime.now()
rec_dat     = now_time.strftime('%Y%m%d')
if __name__ == '__main__':
#PRD
        bali_csv_path = '/bfdata/sftp/aoisftp/upload_bali/'
        tmp_aoi_csv_path = '/data/buffer/kudu_aoi_csv_bail_tmp/'
        tmp_kckf_csv_path = '/data/buffer/kudu_kckf_csv_bail_tmp/'
        tmp_othertest_csv_path = '/data/buffer/kudu_othertest_csv_bail_tmp/'
#        tmp_shim_csv_path = '/data/buffer/kudu_shim_csv_bail_tmp/'
        err_bail_aoi_csv_path = '/data/history/csv/aoi_csv/err_bali_aoi_csv_kudu/'+rec_dat+'/'
        err_bail_kckf_csv_path = '/data/history/csv/kckf_csv/err_bali_kckf_csv_kudu/'+rec_dat+'/'
        err_bail_csv_path = '/data/history/csv/err_bali_csv/'+rec_dat+'/'
        err_bail_othertest_csv_path ='/data/history/csv/othertest_csv/err_bali_othertest_csv_kudu/'+rec_dat+'/'
        
#Dev
#        bali_csv_path = '/bfdata/sftp/aoisftp/dev_upload_bali/'
#        tmp_aoi_csv_path = '/data/buffer/kudu_aoi_csv_bail_tmp/'
#        tmp_kckf_csv_path = '/data/buffer/kudu_kckf_csv_bail_tmp/'
#        err_bail_aoi_csv_path = '/data/history/csv/aoi_csv/err_bali_aoi_csv_kudu/'+rec_dat+'/'
#        err_bail_kckf_csv_path = '/data/history/csv/kckf_csv/err_bali_kckf_csv_kudu/'+rec_dat+'/'
#        err_bail_csv_path = '/data/history/csv/err_bali_csv/'+rec_dat+'/'
####
        if os.path.exists(err_bail_aoi_csv_path) == False:
            os.mkdir(err_bail_aoi_csv_path)
        if os.path.exists(err_bail_kckf_csv_path) == False:
            os.mkdir(err_bail_kckf_csv_path)
        if os.path.exists(err_bail_csv_path) == False:
            os.mkdir(err_bail_csv_path)
        if os.path.exists(err_bail_othertest_csv_path) == False:
            os.mkdir(err_bail_othertest_csv_path)
#    total_jpg_path = '/data/buffer/debug_total_jpg/lost'
#    while 1:
        bali_file_list = [a for a in os.listdir(bali_csv_path) if '.csv' in a]
        time.sleep(10)
        bali_list_len = 0
        if len(bali_file_list) > 3000:
            bali_list_len = 3000
            n = bali_list_len
        else:
            bali_list_len = len(bali_file_list)
            n = bali_list_len
        for i in range(bali_list_len):
            bali_csv = bali_file_list[i]
            #ok_jpg = err_jpg.replace(' ','')
            #ok_jpg = bali_jpg
            #foldernum = str(random.randint(1,5))
            print bali_csv
            filetype = ''
#aoi csv
            if '_AOI1_' in bali_csv:
                print i,"/",n,"moving aoi 1",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI3_' in bali_csv:
                print i,"/",n,"moving aoi 2",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI4_' in bali_csv:
                print i,"/",n,"moving aoi 3",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI6_' in bali_csv:
                print i,"/",n,"moving aoi 4",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI16_' in bali_csv:
                print i,"/",n,"moving aoi 5",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI7_' in bali_csv:
                print i,"/",n,"moving aoi 6",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI8_' in bali_csv:
                print i,"/",n,"moving aoi 7",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_UK1_' in bali_csv:
                print i,"/",n,"moving aoi 8",bal_csv
                filetype = 'normal_aoi_csv'
            elif '_AOI5_' in bali_csv:
                print i,"/",n,"moving aoi 9",bali_csv
                filetype = 'normal_aoi_csv'
            elif '_UT_' in bali_csv:
                print i,"/",n,"moving aoi 10",bali_csv
                filetype = 'normal_aoi_csv'
#kckf csv
            elif 'KEY-FORCE' in bali_csv:
                print i,"/",n,"moving kckf csv",bali_csv
                filetype = 'normal_kckf_csv'
            elif 'KEY-FORCE' in bali_csv:
                print i,"/",n,"moving kckf csv",bali_csv
                filetype = 'normal_kckf_csv'
            elif 'KEY-NOISE' in bali_csv:
                print i,"/",n,"moving kckf csv",bali_csv
                filetype = 'normal_kckf_csv'
            elif 'RUBBER' in bali_csv:
                print i,"/",n,"moving kckf csv",bali_csv
                filetype = 'normal_kckf_csv'
#shim csv
#othertest csv=rel csv
            elif '_LED-CAL_' in bali_csv:
                print i,"/",n,"moving 1 BL_CAL",bali_csv
                filetype = 'normal_othertest_csv'
            elif '_ColorDifference' in bali_csv:
                print i,"/",n,"moving 1 BL_CAL",bali_csv
                filetype = 'normal_othertest_csv'
            elif 'KeyPress_' in bali_csv:
                print i,"/",n,"moving 2 FCT",bali_csv
                filetype = 'normal_othertest_csv'
            elif '_LEDA_' in bali_csv:
                print i,"/",n,"moving 3 BL_Color",bali_csv
                filetype = 'normal_othertest_csv'

#error csv
#            else:
#                filetype = 'Error_bali_csv'
            print 'filetype =',filetype
#check and move aoi csv
            if filetype == 'normal_aoi_csv':
                 try:
                     if bali_csv.count("_") > 4:
                         csv_file_name_pre = bali_csv.split("_",1)[1]
                         if csv_file_name_pre > 41:
                             SN    = bali_csv.split("_")[-2].split("-")[-1]
                             if SN > 16:
                                 foldernum = str(random.randint(1,5))
                                 shutil.move(bali_csv_path+bali_csv,tmp_aoi_csv_path+foldernum+os.sep+bali_csv)
                             else:
                                 filetype = 'Error_bali_aoi_csv'
                         else:
                             filetype = 'Error_bali_aoi_csv'                  
                     else:
                         filetype = 'Error_bali_aoi_csv'
                 except Exception,e:
                     print e
                     filetype = 'Error_bali_aoi_csv'
#check and move kckf csv
            if filetype == 'normal_kckf_csv':
                try:
                    if bali_csv.count("_") > 4:
                        csv_file_name_pre = bali_csv.split("_",1)[1]
                        if csv_file_name_pre > 41:
                            SN    = bali_csv.split("_")[-2].split("-")[-1]
                            if SN > 16:
                                foldernum = str(random.randint(1,5))
                                shutil.move(bali_csv_path+bali_csv,tmp_kckf_csv_path+foldernum+os.sep+bali_csv)
                            else:
                                filetype = 'Error_bali_kckf_csv'
                        else:
                                filetype = 'Error_bali_kckf_csv'
                    else:
                        filetype = 'Error_bali_kckf_csv'
                except Exception,e:
                    print e
                    filetype = 'Error_bali_aoi_csv'
#check and move othertest csv
            if filetype == 'normal_othertest_csv':
                try:
                    if bali_csv.count("_") > 4:
                        csv_file_name_pre = bali_csv.split("_",1)[1]
                        if csv_file_name_pre > 41:
                            SN    = bali_csv.split("_")[-5]
                            if SN > 16:
                                foldernum = str(random.randint(1,5))
                                shutil.move(bali_csv_path+bali_csv,tmp_othertest_csv_path+foldernum+os.sep+bali_csv)
                            else:
                                filetype = 'Error_bali_othertest_csv'
                        else:
                                filetype = 'Error_bali_othertest_csv'
                    else:
                        filetype = 'Error_bali_othertest_csv'
                except Exception,e:
                    print e
                    filetype = 'Error_bali_othertest_csv'

#move errcsv
            if filetype == 'Error_bali_aoi_csv':
                print "moving Error bali aoi csv",bali_csv
                shutil.move(bali_csv_path+bali_csv,err_bail_aoi_csv_path+bali_csv)      
            if filetype == 'Error_bali_kckf_csv':
                print "moving Error bali kckf csv",bali_csv
                shutil.move(bali_csv_path+bali_csv,err_bail_kckf_csv_path+bali_csv)
            if filetype == 'Error_bali_othertest_csv':
                print "moving Error bali othertest csv",bali_csv
                shutil.move(bali_csv_path+bali_csv,err_bail_othertest_csv_path+bali_csv)
            if filetype == 'Error_bali_csv':
                print "moving Error bali csv",bali_csv
                shutil.move(bali_csv_path+bali_csv,err_bail_csv_path + bali_csv)

end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
#print "Job: 01"
print "Total files count: ",n
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",n / (end_time - start_time),"pcs/s,at date/time: ",com_dat
