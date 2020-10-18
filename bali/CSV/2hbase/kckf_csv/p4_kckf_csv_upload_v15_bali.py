import os,time,happybase,datetime,shutil,MySQLdb
from hdfs import InsecureClient

start_time  = time.time()
now_time    = datetime.datetime.now()
rec_dat     = now_time.strftime('%Y%m%d')
source_path = '/data/buffer/hbase_bali_kckf_csv/'
#source_path = '/bfdata/sftp/aoisftp/kckf_csv/'
#source_path = '/p4data/sftp/aoisftp/kckf_csv/'
#err_path    = '/data/history/csv/kckf_csv/csv_err/'
err_path    = '/data/history/csv/kckf_csv/err_bali_kckf_csv_hbase/'+rec_dat+'/'
log_path    = '/home/armap/log_exc/'
#archive_path= '/data/history/csv/kckf_csv/'+rec_dat+'/'
#archive_path= '/hdata/history/csv/kckf_csv/'+rec_dat+'/'
archive_path= '/data/history/csv/kckf_csv/'+rec_dat+'/'

if os.path.exists(archive_path) == False:
    os.mkdir(archive_path)
if os.path.exists(err_path) == False:
    os.mkdir(err_path)

job_name    = 'p4_bail_kckf_csv_upload_hbase'
run_log     = log_path + job_name +'_' +  rec_dat +'.record'
exc_log     = open(run_log,"a+")

## For HBase connection
connection = happybase.Connection('10.41.158.65')
table = connection.table('p8_aoi_csv')

csv_file = os.listdir(source_path)
time.sleep(5)

n = 0
if len(csv_file) > 20000:
    n = 20000
else:
    n = len(csv_file)

for i in range(n):
    temp  = {}
    print i,"of",n,csv_file[i]
#    if "  " in csv_file[i].split("|")[-1]:
#        if csv_file[i].count("@") == 1:
    if csv_file[i].count("_") > 4:
        if '|Keyfeel Data|J680|' not in csv_file[i]:
                csv_file_name_pre = csv_file[i].split("_",1)[1].replace(".csv","")
                if len(csv_file_name_pre) >16:
                    SN    = csv_file[i].split("_")[-4]
            #if ('MODEL=X10' not in csv_file[i]) and ('MODEL=X13' not in csv_file[i]):
                    if len(SN) > 15:
                        if 'KEY-NOISE' in csv_file[i]:
                            cf  = 'KN'
                        elif 'RUBBER' in csv_file[i]:
                            cf  = 'PR'
                        elif 'PRE-KEY-FORCE' in csv_file[i]:
                            cf  = 'ISAKF'
                        elif ('KEY-FORCE' in csv_file[i]) and ('PRE-KEY-FORCE' not in csv_file[i]):
                            cf  = 'KCKF'
                        elif 'BACK' in csv_file[i]:
                            cf  = 'KCKF_R'
                        else:
                            cf  = 'KCKF'
                        value = open(source_path + csv_file[i]).read()
                        pre_CDATE = csv_file[i].split("|")[-1].split("_")[-3]
                        if '.' in pre_CDATE:
                            CDATE = csv_file[i].split('|')[-1].split("_")[-3].split(".")[0].replace('-','').replace(' ','')
                        else:
                            CDATE = pre_CDATE
#      cdate = CDATE[:4] +"-"+ CDATE[4:6] +"-"+ CDATE[6:8] +" "+ CDATE[8:10] +":"+ CDATE[10:12] +":"+ CDATE[12:]
      #cf    = 'KCKF'
                        print "file OK:"+SN,CDATE,cf
                        temp[cf + ":" + CDATE ] = value
                        table.put(SN,temp)
                        exc_log.write("upload csv: " + csv_file[i] + " complete." + "\n")
                        shutil.move(source_path+csv_file[i],archive_path+csv_file[i])
                    else:
                        shutil.move(source_path+csv_file[i],err_path+csv_file[i])
                        print "file NG:"+csv_file[i]
                        exc_log.write("fail upload csv: " + csv_file[i] + " ." + "\n")
                else:
                    shutil.move(source_path+csv_file[i],err_path+csv_file[i])
                    print "file NG:"+csv_file[i]
#                    exc_log.write("fail upload csv: " + csv_file[i] + " ." + "\n")
#            else:
#                shutil.move(source_path+csv_file[i],err_path+csv_file[i])
#                print "file NG:"+csv_file[i]
#                exc_log.write("fail upload csv: " + csv_file[i] + " ." + "\n")
        else:
            shutil.move(source_path+csv_file[i],err_path+csv_file[i])
            print "file NG:"+csv_file[i]
            exc_log.write("fail upload csv: " + csv_file[i] + " ." + "\n")
    else:
        shutil.move(source_path+csv_file[i],err_path+csv_file[i])
        print "file NG:"+csv_file[i]
        exc_log.write("fail upload csv: " + csv_file[i] + " ." + "\n")
#    folder1_name = csv_file[i].split("|")[0][:7]
#    folder2_name = csv_file[i].split("|")[4].split("=")[1]
#    folder3_name = csv_file[i].split("|")[7].split("_")[1]

#    nfolder1_name = csv_file[i].split("|")[0][:7]
#    nfolder2_name = csv_file[i].split("|")[7].split("_")[1]
#    nfolder3_name = csv_file[i].split("|")[4].split("=")[1] + "_" + csv_file[i].split("|")[5]
#    print nfolder1_name,nfolder2_name,nfolder3_name 
#    if os.path.exists(target_path+folder1_name) == False:
#        os.mkdir(target_path+folder1_name)
#    if os.path.exists(target_path+folder1_name+"/" + folder2_name) == False:
#        os.mkdir(target_path+folder1_name+"/" + folder2_name)
#    if os.path.exists(target_path+folder1_name+"/" + folder2_name + "/" + folder3_name) == False:
#        os.mkdir(target_path+folder1_name+"/" + folder2_name + "/" + folder3_name)

#    if os.path.exists(orig_path+nfolder1_name) == False:
#        os.mkdir(orig_path+nfolder1_name)
#    if os.path.exists(orig_path+nfolder1_name+"/" + nfolder2_name) == False:
#        os.mkdir(orig_path+nfolder1_name+"/" + nfolder2_name)
#    if os.path.exists(orig_path+nfolder1_name+"/" + nfolder2_name + "/" + nfolder3_name) == False:
#        os.mkdir(orig_path+nfolder1_name+"/" + nfolder2_name + "/" + nfolder3_name)

#    os.rename(source_path + csv_file[i],source_path + csv_file[i].replace(":",""))

#    if os.path.exists(target_path + folder1_name +"/" + folder2_name + "/" + folder3_name + "/" + csv_file[i].replace(":","")):
#        shutil.copy(source_path + csv_file[i].replace(":",""),target_path + folder1_name +"/" + folder2_name + "/" + folder3_name + "/" + csv_file[i].split("|")[-1]+"v2" )
#    else:
#        shutil.copy(source_path + csv_file[i].replace(":",""),target_path + folder1_name +"/" + folder2_name + "/" + folder3_name + "/" + csv_file[i].split("|")[-1] )

#    if os.path.exists(orig_path + nfolder1_name +"/" + nfolder2_name + "/" + nfolder3_name + "/" + csv_file[i].replace(":","")):
#        shutil.move(source_path + csv_file[i].replace(":",""),orig_path + nfolder1_name +"/" + nfolder2_name + "/" + nfolder3_name + "/" + csv_file[i].split("|")[-1]+"v2" )
#    else:
#        shutil.move(source_path + csv_file[i].replace(":",""),orig_path + nfolder1_name +"/" + nfolder2_name + "/" + nfolder3_name + "/" + csv_file[i].split("|")[-1] )

exc_log.close()
connection.close()

end_time = time.time()
com_dat     = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
print "Job:",job_name
print "Total files count: ",n
print "Total time:      ", end_time - start_time,"s"
print "Speed: ",n / (end_time - start_time),"pcs/s,at date/time: ",com_dat
print
