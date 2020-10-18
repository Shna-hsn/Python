import os,time,happybase,datetime,shutil,MySQLdb
from hdfs import InsecureClient

start_time  = time.time()
now_time    = datetime.datetime.now()
rec_dat     = now_time.strftime('%Y%m%d')
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
#source_path = '/bfdata/sftp/aoisftp/csv/'
#source_path = '/p4data/sftp/aoisftp/csv/'
source_path = '/data/buffer/hbase_bali_aoi_csv/'
#source_path = '/hdata/history/csv/aoi_csv/err_aoi_csv/'
#orig_path   = '/p2data/sftp/csvsftp/big_folder/a_csv_file/'
#target_path = '/p2data/sftp/csvsftp/big_folder/b_csv_file/'
#archive_path= '/data/history/csv/aoi_csv/' + rec_dat + '/'
archive_path= '/data/history/csv/aoi_csv/' + rec_dat + '/'
#err_path    = '/data/history/csv/aoi_csv/err_csv/'
err_path    = '/data/history/csv/aoi_csv/err_bali_aoi_csv_hbase/' + rec_dat + '/'
job_name    = 'p4_bali_aoi_csv_upload_hbase'
run_log     = '/home/armap/log_exc/'+ job_name +'_' +  rec_dat +'.record'
exc_log     = open(run_log,"a+")

if os.path.exists(archive_path) == False:
    os.mkdir(archive_path)
if os.path.exists(err_path) == False:
    os.mkdir(err_path)

## For HBase connection
connection = happybase.Connection('10.41.158.65')
table = connection.table('p8_aoi_csv')

## For MYSQL connection 
#db = MySQLdb.connect("10.41.158.65","root","admfcs","aoi_mo_sn" )
#cursor = db.cursor()

csv_file = os.listdir(source_path)
time.sleep(5)
if len(csv_file) >20000:
    n = 20000
else:
    n = len(csv_file)

for i in range(n):
    print i,"of",n
#  if "  " in csv_file[i].split("|")[-1]:
#    shutil.move(source_path + csv_file[i],err_path + csv_file[i])
#  else:
    if "_" not in csv_file[i].split("|")[-1]:
        print err_path
        print csv_file[i]
        shutil.move(source_path + csv_file[i],err_path + csv_file[i])
    else:
        if "|NoSFC|" not in csv_file[i]:
            if "~lock" not in csv_file[i]:
              if "VoidBlob_PG" not in csv_file[i]:
                if "VoidBlobResult_PG"  not in csv_file[i]:
                  if "|VoidCoverArea_PG|" not in csv_file[i]:
                    if "CalDistanceResult_PG" not in csv_file[i]:
                      if "|KeyCap_PG_Height|" not in csv_file[i]:
                        if "|KeyCap_PG_XY|" not in csv_file[i]:
                          if "|Character_PG|" not in csv_file[i]:
#                        if "ISO_PSA" not in csv_file[i]:
                            if "Laser3D_PG" not in csv_file[i]:
                                csv_file_name_pre = csv_file[i].split("_",1)[1]
                                if csv_file_name_pre > 41:
                                    temp  = {}
                                    value = open(source_path + csv_file[i]).read()
#    MO    = csv_file[i].split("|")[3].split("_")[0]
                                    SN    = csv_file[i].split("_")[-2].split("-")[-1]
                                    if SN > 16:
                                        CDATE = csv_file[i].split("_")[-1].split(".")[0]
#    cdate = CDATE[:4] +"-"+ CDATE[4:6] +"-"+ CDATE[6:8] +" "+ CDATE[8:10] +":"+ CDATE[10:12] +":"+ CDATE[12:]
#    func  = csv_file[i].split("|")[2]
                                        cf = ''
                                        if '3DVoidCoverArea_PG-BACK' in csv_file[i] or 'NewHS_PG-BACK' in csv_file[i]:
                                            cf = 'KB_R'
                                        elif '_AOI1_'    in csv_file[i]:
                                            cf = 'PSA'
                                        elif '_AOI3_'    in csv_file[i]:
                                            cf = 'BF'
                                        elif '_AOI4_'    in csv_file[i]:
                                            cf = 'AHS'
                                        elif '_AOI16_'   in csv_file[i] or '_AOI6_'   in csv_file[i]:
                                            cf = 'MUK'
                                        elif '_AOI7_'    in csv_file[i]:
                                            cf = 'KB'
                                        elif '_AOI8_'    in csv_file[i]:
                                            cf = 'HT'
                                        elif '_UK1_'  in csv_file[i] or '_AOI5_' in csv_file[i]:
                                            cf = 'MUK_PSA'
                                        elif '3DVoidCoverArea_PG'    in csv_file[i] or 'NewHS_PG' in csv_file[i]:
                                            cf = 'KB'
                                        elif 'ISA_PG-BACK'           in csv_file[i]:
                                            cf = 'BF_R'
                                        elif 'ISA_PG'                in csv_file[i]:
                                            cf = 'BF'
                                        elif 'ShiftRotate_PG-BACK'   in csv_file[i]:
                                            cf = 'PSA_R'
                                        elif 'ShiftRotate_PG'        in csv_file[i]:
                                            cf = 'PSA'
                                        elif 'NEWFPPSA_PG'           in csv_file[i]:
                                            cf = 'MR'
                                        elif 'ISAH_PG-BACK'          in csv_file[i]:
                                            cf = 'AHS_R'
                                        elif 'ISAH_PG'               in csv_file[i]:
                                            cf = 'AHS'
                                        elif 'XY_HT_PG-BACK'         in csv_file[i]:
                                            cf = 'HT_R'
                                        elif 'XY_HT_PG'              in csv_file[i]:
                                            cf = 'HT'
                                        elif 'DisXYZ_PG-BACK'        in csv_file[i] or 'Stiffener_PG-BACK' in csv_file[i]:
                                            cf = 'WF_R'
                                        elif ('Stiffener'            in csv_file[i]) and 'TEST_BUCK' not in csv_file[i]:
                                            cf = 'WF'
                                        elif 'TEST_BUCK'             in csv_file[i]:
                                            cf = 'TEST_BUCK'
                                        elif 'Shim3D_PG'             in csv_file[i]:
                                            cf = 'SHIM'
                                        elif 'NewISA_PG-BACK'        in csv_file[i]:
                                            cf = 'UIC_R'
                                        elif 'NewISA_PG'             in csv_file[i]:
                                            cf = 'UIC'
                                        elif 'NewISAH_PG-BACK'       in csv_file[i]:
                                            cf = 'HS_R'
                                        elif 'NewISAH_PG'            in csv_file[i]:
                                            cf = 'HS'
                                        elif 'FP_PG'                 in csv_file[i]:
                                            cf = 'FP'
                                        elif 'EMR_PG'                in csv_file[i] or 'DisXYZ_PG' in csv_file[i]:
                                            cf = 'EMR'
                                        elif 'IR_PG-BACK'            in csv_file[i]:
                                            cf = 'CO_R'
                                        elif 'IR_PG'                 in csv_file[i]:
                                            cf = 'CO'
                                        elif 'PRE-KEY-FORCE'         in csv_file[i]:
                                            cf = 'KF'
                                        elif 'IMUKPSA_PG'           in csv_file[i]:
                                            cf = 'MUK_PSA'
                                        elif 'FMUK_PG'               in csv_file[i]:
                                            cf = 'MUK'
                                        elif 'MUK_TRAY'               in csv_file[i]:
                                            cf = 'MUKTRAY'
                                        elif 'UD_PG'                 in csv_file[i]:
                                            cf = 'UD'
                                        elif 'LD_PCB_PG'             in csv_file[i]:
                                            cf = 'FB'
                                        elif 'LD_PSA_PG'             in csv_file[i]:
                                            cf = 'LD'
                                        elif 'HS_PG'                 in csv_file[i]:
                                            cf = 'FF'
                                        elif 'FA_PG'                 in csv_file[i]:
                                            cf = 'FA'
                                        elif 'FG_PG'                 in csv_file[i]:
                                            cf = 'FG'
                                        if cf != "":
                                            temp[cf + ":" + CDATE ] = value
                                            print "file OK:"+SN,CDATE,cf
                                            table.put(SN,temp)
                                            shutil.move(source_path + csv_file[i],archive_path + csv_file[i])
                                            exc_log.write("upload csv: " + csv_file[i] + " complete." + "\n")
                                        else:
                                            shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                                            print "file NG:"+csv_file[i]
                                    else:
                                        shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                                        print "file NG:"+csv_file[i]
                                else:
                                    shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                                    print "file NG:"+csv_file[i]
                            else:
                                shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                                print "file NG:"+csv_file[i]
                          else:
                              shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                              print "file NG:"+csv_file[i]
                        else:
                            shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                            print "file NG:"+csv_file[i]
                      else:
                          shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                          print "file NG:"+csv_file[i]
                    else:
                        shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                        print "file NG:"+csv_file[i]
                  else:
                      shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                      print "file NG:"+csv_file[i]                  
                else:
                    shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                    print "file NG:"+csv_file[i]
              else:
                  shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                  print "file NG:"+csv_file[i]
            else:
                shutil.move(source_path + csv_file[i],err_path + csv_file[i])
                print "file NG:"+csv_file[i]
        else:
            shutil.move(source_path + csv_file[i],err_path + csv_file[i]) 
            print "file NG:"+csv_file[i]

exc_log.close()
connection.close()

end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
print "Job:",job_name
print "Total files count: ",len(csv_file)
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",len(csv_file) / (end_time - start_time),"pcs/s,at date/time: ",com_dat
print
