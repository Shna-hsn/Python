import os,time,happybase,datetime,shutil,re
from hdfs import InsecureClient
from impala.dbapi import connect
from datetime import date, timedelta
import cx_Oracle
import sys

def getcsv():
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql = """SELECT * FROM
                (SELECT *
                FROM V_AOICSV_NAME1
                UNION ALL
                SELECT * FROM V_AOICSV_NAME2) where usn in ('FPW0517G0SJQ1GQAU','FPW0512GJ9DQ1GQA3')"""
    cursor1 = connection.cursor ()
    cursor1.execute (sql)
    inputusn = cursor1.fetchall()
    # print(inputusn)
    return inputusn

def sort_csv(usninfo,lines):

    mu_csv = []
    fg_csv = []
    kb_csv = []
    pt_csv = []
    mpt_csv = []
    mkb_csv = []
    fo_csv = []
    fr_csv = []
    fy_csv = []
    mj_csv = []
    uk_csv = []

    for i in range(len(usninfo)):
        if usninfo[i][0] in lines[0]:
            if usninfo[i][1] == 'MU':
                mu_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'FG':
                fg_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'KB':
                kb_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'PT':
                pt_csv.append(usninfo[i][2])
            else:
                pass
        elif usninfo[i][0] in lines[1]:
            if usninfo[i][1] == 'PT':
                mpt_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'KB':
                mkb_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'FO':
                fo_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'FR':
                fr_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'FY':
                fy_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'MJ':
                mj_csv.append(usninfo[i][2])
            elif usninfo[i][1] == 'UK':
                uk_csv.append(usninfo[i][2])
            else:
                pass
        else:
            pass
    return mu_csv,fg_csv,kb_csv,pt_csv,mpt_csv,mkb_csv,fo_csv,fr_csv,fy_csv,mj_csv,uk_csv

def checkkuducsv(usninfo,result,tablename):
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'
    port = 21050
    try:
        conn = connect(host=hostname1,port=port)
    except:
        time.sleep(2)
    try:
        conn = connect(host=hostname2,port=port)
    except:
        time.sleep(2)
        conn = connect(host=hostname3,port=port)
    finally:
        cur1 = conn.cursor()
        for i in range(len(result)):
            # print('i ' + str(i))
            usnlist = ''
            kuduresult = []
            k = 0
            x = 0
            # print(str(i) + ': ' + str(len(result[i])))
            for j in range(len(result[i])):
                if  k == 9000:
                    # print('j ' + str(j))
                    # print('k ' + str(k))
                    if bool(usnlist):
                        x += 1
                        sql = "select usn,count(distinct usn) from "+tablename[i]+ " where keynumber = '1' and usn in " + '(' + usnlist + ')' + ' group by usn'
                        # print('2' + sql)
                        try:
                            cur1 = conn.cursor()
                            cur1.execute(sql)
                            kuduresult = cur1.fetchall()

                        except Exception as e:
                            print(e)
                        #print(len(kuduresult),len(result[i]))
                        for K in range(len(kuduresult)):
                            if kuduresult[K][0] in result[i]:
                                result[i].remove(kuduresult[K][0])
                            else:
                                pass
                        k = 0
                        usnlist = "''"
                k += 1 # k == 9000

                if j == 0:
                    usnlist = "'" + result[i][0] + "'"
                elif j < 9000: # j == 8999
                    usnlist = usnlist + ',' + "'" + result[i][j] + "'"
                else: # j >= 9000
                    j = j - x*9000
                    # print(j)
                    usnlist = usnlist + ',' + "'" + result[i][j] + "'"

            if bool(usnlist):
                sql = "select usn,count(distinct usn) from "+tablename[i]+ " where keynumber = '1' and usn in " + '(' + usnlist + ')' + ' group by usn'
                # print('1' + sql)
                try:
                    cur1 = conn.cursor()
                    cur1.execute(sql)
                    kuduresult = cur1.fetchall()

                except Exception as e:
                    print(e)
                #print(len(kuduresult),len(result[i]))
                for K in range(len(kuduresult)):
                    if kuduresult[K][0] in result[i]:
                        result[i].remove(kuduresult[K][0])
                    else:
                        pass
        #print(result)
        conn.close()
    return result

def movefile(finallyresult):
    rec_dat = (date.today() + timedelta(days = -2)).strftime("%Y%m%d")
    csvstage = ['MU','FG','KB','PT','PT','KB','FO','FR','FY','MJ','UK']
    func = ['MB_PG','TOP_PG','BOT_PG','XY_HT_PG','XY_HT_PG','3DVoidCoverArea_PG','ShiftRotate_PG','ISAH_PG','ISA_PG','FMUK_PG','IMUKPSA_PG']
    # source_path = '/hdata/sftp/aoisftp/csv/'
    err_path = '/kdata/history/csv/aoi_csv/err_aoi_csv_sftp/'+rec_dat+'/'

    # file_folder = '/kdata/buffer/kudu_aoi_csv_tmp/1/'
    errorfolder ='/kdata/history/csv/aoi_csv/err_aoi_csv_kudu/'+rec_dat+'/'
    # print(finallyresult)
    for i in range(len(finallyresult)):
        if bool(finallyresult[i]):
            csv_file = [a for a in os.listdir(err_path) if '.csv' in a and func[i] in a]
            csv_file1 = [a for a in os.listdir(errorfolder) if '.csv' in a and func[i] in a]
            for j in range(len(finallyresult[i])):
                for k in range(len(csv_file)):
                    if finallyresult[i][j] in csv_file[k]:
                        print(csvstage[i] + ' file loss kudu: ' + err_path + csv_file[k])
                        # shutil.move(err_path + csv_file[k],source_path + csv_file1[k])
                for k in range(len(csv_file1)):
                    if finallyresult[i][j] in csv_file1[k]:
                        print(csvstage[i] + ' file loss kudu: ' + errorfolder + csv_file1[k])
                        # shutil.move(errorfolder + csv_file1[k],file_folder + csv_file1[k])

if __name__ == "__main__":

    usninfo = getcsv()
    #print(usninfo)
    lines = [('TB1-1FT-03','TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16'),('TB1-1FT-01','TB1-1FT-02')]
    #csvstage = [('MU','FG','KB','PT'),('PT','KB','FO','FR','FY','MJ','UK')]
    tablename = ['allie.aoi_mb','allie.aoi_top','allie.aoi_bot','allie.aoi_ht','allie.aoi_ht','allie.aoi_KB','allie.aoi_psa','allie.aoi_ahs','allie.aoi_bf','allie.aoi_muk','allie.aoi_muk_psa']      
    result = sort_csv(usninfo,lines)
    # print(result)
    losskudu = checkkuducsv(usninfo,result,tablename)
    finallyresult = checkkuducsv(usninfo,losskudu,tablename)
    # print(finallyresult)
    for i in range(len(finallyresult)):
        usnlist = ''
        if len(finallyresult[i]) != 0:
            for j in range(len(finallyresult[i])):
                if j == 0:
                    usnlist = "'" + finallyresult[i][0] + "'"
                else:
                    usnlist = usnlist + ',' + "'" + finallyresult[i][j] + "'"
            sql = "select usn,count(distinct usn) from "+tablename[i]+ " where keynumber = '1' and usn in " + '(' + usnlist + ')' + ' group by usn'
            print(sql)