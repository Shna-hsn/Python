import os,datetime,time,shutil
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def aoi_jpg_migrate(aoi_jpg):
    if aoi_jpg.count("_") >= 2:
        if aoi_jpg.count("@") == 1:
            jpg_file_name_pre = aoi_jpg.split("@")[1]
            if len(jpg_file_name_pre) >16:
                if jpg_file_name_pre.count("_") < 1:
                    if jpg_file_name_pre.count("-") >= 1:
                        new_name_pre = aoi_jpg.split("_")
                        #print aoi_jpg
                        if new_name_pre[-1].split("@")[1].count('-') >= 2:
                            sn = new_name_pre[-1].split("@")[1].split("-",1)[1].replace(".JPG","")
                        else:
                            sn = new_name_pre[-1].split("@")[1].split("-")[1].replace(".JPG","")
                        if len(sn) > 16:
                            func = new_name_pre[0].replace(' ','')
                            host = new_name_pre[-1].split("@")[0]
                            ctime = new_name_pre[-1].split("@")[1].split("-")[0]
                            dt = new_name_pre[-1].split("@")[1].split("-")[0][:8]
                            if len(dt)== 8:
                                if sn.startswith('CMX'):
                                   func1 =new_name_pre[0].replace(' ','')
                                   func2 =new_name_pre[2].replace(' ','')
                                   new_name = func1 + "-" + func2 + "_" + dt + "_" + host + "@" + ctime +"-"+ sn + ".JPG"
                                   print "CMX-newname:"+new_name
                                else:
                                    new_name = func + "_" + dt + "_" + host + "@" + ctime +"-"+ sn + ".JPG"
                                if "-UD_" in new_name:
                                    filetype='carrier'                                
                                elif "-LD_" in new_name:
                                    filetype='carrier'
                                elif "-FB_" in new_name:
                                    filetype='carrier'
                                elif "-FG_" in new_name:
                                    filetype='carrier'
                                elif "-FA_" in new_name:
                                    filetype='carrier'
                                elif "-HS_PG" in new_name:
                                    filetype='carrier'
                                elif ".JPG_" in new_name:
                                    filetype='errJPG'
                                else:
                                    filetype='normal'
                                if filetype == 'normal':
                                    shutil.move(aoi_jpg_path+aoi_jpg,total_jpg+new_name)
                                    print "move to normal:",aoi_jpg
                                if filetype == 'carrier':
                                    shutil.move(aoi_jpg_path+aoi_jpg,carrier_jpg_path+new_name)
                                    print "move to carrier",aoi_jpg
                                #print "o_aoi_ok_name:",new_name
                            else:
                                filetype='errJPG'
                                print "o_aoi_ng_name -err dt:",aoi_jpg
                        else:
                            filetype='errJPG'
                            print "o_aoi_ng_name 1:",aoi_jpg
                    else:
                        filetype='errJPG'
                        print "o_aoi_ng_name 2:",aoi_jpg
                else:
                    filetype='errJPG'
                    print "o_aoi_ng_name 3:",aoi_jpg
            else:
                filetype='errJPG'
                print "o_aoi_ng_name 4:",aoi_jpg
        else:
            filetype='errJPG'
            print "o_aoi_ng_name 5:",aoi_jpg
    else:
        filetype='errJPG'
        print "o_aoi_ng_name 6:",aoi_jpg
    if filetype == 'errJPG':
        shutil.move(aoi_jpg_path+aoi_jpg,err_jpg+aoi_jpg)
        print "move to errJPG:",aoi_jpg


if __name__ == "__main__":
    aoi_jpg_path = '/data/sftp/aoisftp/upload/'
    total_jpg = '/data/buffer/total_jpg/'
    carrier_jpg_path = '/data/buffer/total_jpg_carrier/'
    err_jpg = '/data/buffer/err_jpg/'
    ctime = time.ctime()
    stime = time.time()
    start_time = time.time()

    aoi_jpg = [a for a in os.listdir(aoi_jpg_path) if '.JPG' in a]
    time.sleep(5)
    aoi_jpg_n = 0
    if len(aoi_jpg) > 60000:
        aoi_jpg_n = 60000
    else:
        aoi_jpg_n = len(aoi_jpg)

    for a in range(aoi_jpg_n):
        print a,"of",aoi_jpg_n
        aoi_jpg_migrate(aoi_jpg[a])

    end_time = time.time()
    com_dat  = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
    print "jog: 01jpg_migrate"
    print "Total files count: ",aoi_jpg_n
    print "Total time:      ", end_time - start_time,"s"
    print "Complete. Speed: ", aoi_jpg_n / (end_time - start_time), "pcs/s,at date/time:",com_dat

