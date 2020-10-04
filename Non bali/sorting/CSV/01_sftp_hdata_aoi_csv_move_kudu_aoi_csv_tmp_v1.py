import os,datetime,time,shutil
import random

start_time = time.time()
now_time    = datetime.datetime.now()
rec_dat     = now_time.strftime('%Y%m%d')
if __name__ == '__main__':
#    err_jpg_path = '/data/buffer/err_jpg/'
    #err_jpg_path = '/data/buffer/debug_total_jpg/lost/X3A-HT-XY-LEGEND/'
    #err_jpg_path = '/data/buffer/debug_total_jpg/lost/X3A-HT-XY-LEGEND_ok/'
        #err_jpg_path = '/data/buffer/debug_total_jpg/today/'
		#csvpath = '/data/buffer/kudu_aoi_csv/'
        #err_jpg_path = '/data/buffer/hight_priority_jpg/'
        #total_jpg_path = '/data/buffer/debug_total_jpg'
#PRD
        source_path = '/hdata/sftp/aoisftp/csv/'
        tmp_csv_path = '/data/buffer/kudu_aoi_csv_tmp/'
        hbase_csv_path = '/data/buffer/hbase_aoi_csv/'
        bali_csv_path = '/bfdata/sftp/aoisftp/upload_bali/'
#Dev
        #source_path = '/hdata/sftp/aoisftp/dev_csv/'
        #source_path = '/data/history/csv/aoi_csv/err_aoi_csv_kudu/20190520_debug/'
        #tmp_csv_path = '/data/buffer/dev_kudu_aoi_csv_tmp/'
        #hbase_csv_path = '/data/buffer/hbase_aoi_csv/'
        #bali_csv_path = '/bfdata/sftp/aoisftp/dev_upload_bali/'
        err_path = '/data/history/csv/aoi_csv/err_aoi_csv_sftp/'+rec_dat+'/'
        err_undefined_path = '/data/history/csv/aoi_csv/err_undefined_aoi_csv_sftp/'+rec_dat+'/'
        if os.path.exists(err_path) == False:
            os.mkdir(err_path)
        if os.path.exists(err_undefined_path) == False:
            os.mkdir(err_undefined_path)
#    total_jpg_path = '/data/buffer/debug_total_jpg/lost'
#    while 1:
        csv_file = [a for a in os.listdir(source_path) if '.csv' in a]
        time.sleep(5)
        csvlist_len = 0
        if len(csv_file) > 1000000:
            csvlist_len = 1000000
            n = csvlist_len
        else:
            csvlist_len = len(csv_file)
            n = csvlist_len
        for i in range(csvlist_len):
            filetype = ''
            try:
                if csv_file[i].startswith('192.'):
                    filetype = 'Bali_csv'
                elif "_" not in csv_file[i].split("|")[-1]:
                    filetype = 'Error_csv'
#                elif "|NoSFC|" in csv_file[i]:
#                    filetype = 'Error_csv'
                elif "|IGERMA|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|MRCALIPER_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "~lock" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|Shim2D_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|VoidBlob_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|JIS_HT_Main|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|VoidBlobResult_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "HT_Repair_" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|VoidCoverArea_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
#                elif "LASER_CUT" in csv_file[i]:
#                    filetype = 'Error_csv'
                elif "|CalDistanceResult_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|IMUK_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|KeyCap_PG_Height|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|KCAttach_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|KeyCap_PG_XY|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|as|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|Character_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|Laser3D_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|OQC_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "|MUKTRAY_PG|" in csv_file[i]:
                    filetype = 'Error_csv'
                elif "-BACK" in csv_file[i]:
                    filetype = 'Error_csv'
#                elif "_ANSI_NH_" in csv_file[i]:
#                    filetype = 'Error_csv'
                #normal
#                elif '3DVoidCoverArea_PG-BACK' in csv_file[i] or 'NewHS_PG-BACK' in csv_file[i]:
#                    filetype = 'normal'
                elif '3DVoidCoverArea_PG'    in csv_file[i] or 'NewHS_PG' in csv_file[i]:
                    filetype = 'normal'
#                elif 'ISA_PG-BACK'           in csv_file[i]:
#                    filetype = 'normal'
                elif 'ISA_PG'                in csv_file[i]:
                    filetype = 'normal'
#                elif 'ShiftRotate_PG-BACK'   in csv_file[i]:
#                    filetype = 'normal'
                elif 'ShiftRotate_PG'        in csv_file[i]:
                    filetype = 'normal'
                elif 'NEWFPPSA_PG'           in csv_file[i]:
                    filetype = 'normal'
#                elif 'ISAH_PG-BACK'          in csv_file[i]:
#                    filetype = 'normal'
                elif 'ISAH_PG'               in csv_file[i]:
                    filetype = 'normal'
#                elif 'XY_HT_PG-BACK'         in csv_file[i]:
#                    filetype = 'normal'
                elif 'XY_HT_PG'              in csv_file[i]:
                    filetype = 'normal'
#                elif 'DisXYZ_PG-BACK'        in csv_file[i] or 'Stiffener_PG-BACK' in csv_file[i]:
#                    filetype = 'normal'
                elif ('Stiffener'            in csv_file[i]) and 'TEST_BUCK' not in csv_file[i]:
                    filetype = 'normal'
                elif 'TEST_BUCK'             in csv_file[i]:
                    filetype = 'normal'
                elif 'Shim3D_PG'             in csv_file[i]:
                    filetype = 'normal'
#                elif 'NewISA_PG-BACK'        in csv_file[i]:
#                    filetype = 'normal'
                elif 'NewISA_PG'             in csv_file[i]:
                    filetype = 'normal'
#                elif 'NewISAH_PG-BACK'       in csv_file[i]:
#                    filetype = 'normal'
                elif 'NewISAH_PG'            in csv_file[i]:
                    filetype = 'normal'
                elif 'FP_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'EMR_PG'                in csv_file[i] or 'DisXYZ_PG' in csv_file[i]:
                    filetype = 'normal'
#                elif 'IR_PG-BACK'            in csv_file[i]:
#                    filetype = 'normal'
                elif 'IR_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'PRE-KEY-FORCE'         in csv_file[i]:
                    filetype = 'normal'
                elif 'IMUKPSA_PG'            in csv_file[i]:
                    filetype = 'normal'
                elif 'FMUK_PG'               in csv_file[i]:
                    filetype = 'normal'
                elif 'MUK_TRAY'              in csv_file[i]:
                    filetype = 'normal'
                elif 'UD_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'LD_PCB_PG'             in csv_file[i]:
                    filetype = 'normal'
                elif 'LD_PSA_PG'             in csv_file[i]:
                    filetype = 'normal'
                elif 'HS_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'FA_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'FG_PG'                 in csv_file[i]:  
                    filetype = 'normal'
                elif 'FP_XY_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'MB_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'TOP_PG'                 in csv_file[i]:
                    filetype = 'normal'
                elif 'BOT_PG'                 in csv_file[i]:
                    filetype = 'normal'
                else:
                    filetype = 'Error_undefined_csv'
                if filetype == 'normal':
                    try:
                        csv_file_name_pre = csv_file[i].split("@")[1].split("|")[-1]
                        if csv_file_name_pre > 25:
                            SN    = csv_file[i].split("|")[-1].split("_")[-2]
                            if SN > 16:
                                buffer_csv = csv_file[i]
                                foldernum = str(random.randint(1,10))
                                kudu_tmp_path = tmp_csv_path+foldernum+os.sep
                                if os.path.exists(kudu_tmp_path) == False:
                                    os.mkdir(kudu_tmp_path)
                                shutil.move(source_path+buffer_csv,tmp_csv_path+foldernum+os.sep+buffer_csv)
                                print i,"/",n,"moving Normal csv",csv_file[i]
                            else:
                                filetype = 'Error_csv'
                        else:
                            filetype = 'Error_csv'
                    except Exception,e:
                        print e
                        filetype = 'Error_csv'
            except Exception,e:
                print e
                filetype = 'Error_csv'
            if filetype == 'Error_csv':
                print "moving Error csv",csv_file[i]
                shutil.move(source_path + csv_file[i],err_path + csv_file[i])
            if filetype == 'Error_undefined_csv':
                print "moving Error undefined csv",csv_file[i]
                shutil.move(source_path + csv_file[i],err_undefined_path + csv_file[i])
            if filetype == 'Bali_csv':
                print "moving Bali csv",csv_file[i]
                shutil.move(source_path + csv_file[i],bali_csv_path + csv_file[i])
end_time = time.time()
end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
print "Job: 01_kudu_kckf_csv_move_to_kudu_kckf_csv_tmp"
print "Total files count: ",n
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",n / (end_time - start_time),"pcs/s,at date/time: ",com_dat
