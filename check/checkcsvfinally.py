import os,time,happybase,datetime,shutil,re
from hdfs import InsecureClient
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from impala.dbapi import connect
from datetime import date, timedelta
import cx_Oracle
import sys

def send_email(body):
    default_encoding = 'utf-8'
    smtpHost = 'wzsowa.wistron.com'
    smtpPort = '587'
    sslPort  = '587'
    fromMail = 'Robbin_Cai@wistron.com'
    toMail   = 'Robbin_Cai@wistron.com,Nander_Huang@wistron.com'
    username = 'Z19033018'
    password = '19033018C...'
    subject  = u'UPLOAD AOI CSV&JPG REPORT'
    encoding = 'utf-8'
    mail = MIMEText(body.encode(encoding),'html',encoding)
    mail['Subject'] = Header(subject,encoding)
    mail['From'] = fromMail
    mail['To'] = ','.join(toMail) if isinstance(toMail,list) else toMail

    try:
        smtp = smtplib.SMTP(smtpHost,smtpPort)
        smtp.starttls()
        smtp.ehlo()
        smtp.login(username,password)
        if not isinstance(toMail,list):
            toMail=toMail.split(",")
        smtp.sendmail(fromMail,toMail,mail.as_string())
        smtp.close()
        print ('OK')
    except Exception as e:
        print(e)

def getcsv():
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql1 = 'SELECT * FROM V_AOICSV_NAME1'
    sql2 = 'SELECT * FROM V_AOICSV_NAME2'
    cursor1 = connection.cursor ()
    cursor1.execute (sql1)
    jpg1 = cursor1.fetchall()
    cursor2 = connection.cursor ()
    cursor2.execute (sql2)
    jpg2 = cursor2.fetchall()
    return jpg1,jpg2

def getmessage():
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql = 'SELECT line,stage,COUNT(usn) FROM V_AOI_FILECOUNT GROUP BY line,stage ORDER BY stage,line'
    cursor = connection.cursor ()
    cursor.execute (sql)
    inputresult = cursor.fetchall()
    return inputresult

def getbystageusn(lines):
    mu_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    fg_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    kb_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    pt_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    csvmessage = getcsv()[0]
    for i in range(len(csvmessage)):
        for j in range(len(lines)):
            if csvmessage[i][0] == lines[j]:
                if csvmessage[i][1] == 'MU':
                    mu_csvname1 = csvmessage[i][2]
                    if mu_csvname[j] == '\' \'':
                        mu_csvname[j] = '\'' + mu_csvname1 + '\''
                    else:
                        mu_csvname[j] = mu_csvname[j] + ',\'' + mu_csvname1 + '\''
                elif csvmessage[i][1] == 'FG':
                    fg_csvname1 = csvmessage[i][2]
                    if fg_csvname[j] == '\' \'':
                        fg_csvname[j] = '\'' + fg_csvname1 + '\''
                    else:
                        fg_csvname[j] = fg_csvname[j] + ',\'' + fg_csvname1 + '\''
                elif csvmessage[i][1] == 'KB':
                    kb_csvname1 = csvmessage[i][2]
                    if kb_csvname[j] == '\' \'':
                        kb_csvname[j] = '\'' + kb_csvname1 + '\''
                    else:
                        kb_csvname[j] = kb_csvname[j] + ',\'' + kb_csvname1 + '\''
                elif csvmessage[i][1] == 'PT':
                    pt_csvname1 = csvmessage[i][2]
                    if pt_csvname[j] == '\' \'':
                        pt_csvname[j] = '\'' + pt_csvname1 + '\''
                    else:
                        pt_csvname[j] = pt_csvname[j] + ',\'' + pt_csvname1 + '\''
            else:pass
    return mu_csvname,fg_csvname,kb_csvname,pt_csvname

def getbystageusn1(lines1):
    #pt,kb,ps,fr,fy,mj,uk
    pt_csvname = ['\' \'','\' \'']
    kb_csvname = ['\' \'','\' \'']
    ps_csvname = ['\' \'','\' \'']
    fr_csvname = ['\' \'','\' \'']
    fy_csvname = ['\' \'','\' \'']
    mj_csvname = ['\' \'','\' \'']
    uk_csvname = ['\' \'','\' \'']
    csvmessage = getcsv()[1]
    for i in range(len(csvmessage)):
        for j in range(len(lines1)):
            if csvmessage[i][0] == lines1[j]:
                if csvmessage[i][1] == 'PT':
                    pt_csvname1 = csvmessage[i][2]
                    if pt_csvname[j] == '\' \'':
                        pt_csvname[j] = '\'' + pt_csvname1 + '\''
                    else:
                        pt_csvname[j] = pt_csvname[j] + ',\'' + pt_csvname1 + '\''
                elif csvmessage[i][1] == 'KB':
                    kb_csvname1 = csvmessage[i][2]
                    if kb_csvname[j] == '\' \'':
                        kb_csvname[j] = '\'' + kb_csvname1 + '\''
                    else:
                        kb_csvname[j] = kb_csvname[j] + ',\'' + kb_csvname1 + '\''
                elif csvmessage[i][1] == 'FO':
                    ps_csvname1 = csvmessage[i][2]
                    if ps_csvname[j] == '\' \'':
                        ps_csvname[j] = '\'' + ps_csvname1 + '\''
                    else:
                        ps_csvname[j] = ps_csvname[j] + ',\'' + ps_csvname1 + '\''
                elif csvmessage[i][1] == 'FR':
                    fr_csvname1 = csvmessage[i][2]
                    if fr_csvname[j] == '\' \'':
                        fr_csvname[j] = '\'' + fr_csvname1 + '\''
                    else:
                        fr_csvname[j] = fr_csvname[j] + ',\'' + fr_csvname1 + '\''
                elif csvmessage[i][1] == 'FY':
                    fy_csvname1 = csvmessage[i][2]
                    if fy_csvname[j] == '\' \'':
                        fy_csvname[j] = '\'' + fy_csvname1 + '\''
                    else:
                        fy_csvname[j] = fy_csvname[j] + ',\'' + fy_csvname1 + '\''
                elif csvmessage[i][1] == 'MJ':
                    mj_csvname1 = csvmessage[i][2]
                    if mj_csvname[j] == '\' \'':
                        mj_csvname[j] = '\'' + mj_csvname1 + '\''
                    else:
                        mj_csvname[j] = mj_csvname[j] + ',\'' + mj_csvname1 + '\''
                elif csvmessage[i][1] == 'UK':
                    uk_csvname1 = csvmessage[i][2]
                    if uk_csvname[j] == '\' \'':
                        uk_csvname[j] = '\'' + uk_csvname1 + '\''
                    else:
                        uk_csvname[j] = uk_csvname[j] + ',\'' + uk_csvname1 + '\''
            else:pass
    return pt_csvname,kb_csvname,ps_csvname,fr_csvname,fy_csvname,mj_csvname,uk_csvname

def checkkuducsv(tablename,stage_csvusn,lines,tablename1,stage_csvusn1,lines1):
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'
    port = 21050
    muresult = ''
    fgresult = ''
    kbresult = ''
    ptresult = ''
    #pt,kb,FO,fr,fy,mj,uk
    mptresult = ''
    mkbresult = ''
    mpsresult = ''
    mfrresult = ''
    mfyresult = ''
    mmjresult = ''
    mukresult = ''

    musql = ''
    fgsql = ''
    kbsql = ''
    ptsql = ''
    #pt,kb,FO,fr,fy,mj,uk
    mptsql = ''
    mkbsql = ''
    mpssql = ''
    mfrsql = ''
    mfysql = ''
    mmjsql = ''
    muksql = ''

    #for i in range(len(tablename)):
    for j in range(len(lines)):
        if j == 0:
            musql = "select count(distinct usn) from "+tablename[0]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[0][j] + ')' 
            fgsql = "select count(distinct usn) from "+tablename[1]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[1][j] + ')' 
            kbsql = "select count(distinct usn) from "+tablename[2]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[2][j] + ')' 
            ptsql = "select count(distinct usn) from "+tablename[3]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[3][j] + ')' 
        else:
            musql = musql + " union all " + "select count(distinct usn) from "+tablename[0]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[0][j] + ')' 
            fgsql = fgsql + " union all " + "select count(distinct usn) from "+tablename[1]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[1][j] + ')' 
            kbsql = kbsql + " union all " + "select count(distinct usn) from "+tablename[2]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[2][j] + ')' 
            ptsql = ptsql + " union all " + "select count(distinct usn) from "+tablename[3]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn[3][j] + ')' 
    for j in range(len(lines1)):
        if j ==0:
            mptsql = "select count(distinct usn) from "+tablename1[0]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[0][j] + ')' 
            mkbsql = "select count(distinct usn) from "+tablename1[1]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[1][j] + ')' 
            mpssql = "select count(distinct usn) from "+tablename1[2]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[2][j] + ')' 
            mfrsql = "select count(distinct usn) from "+tablename1[3]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[3][j] + ')' 
            mfysql = "select count(distinct usn) from "+tablename1[4]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[4][j] + ')' 
            mmjsql = "select count(distinct usn) from "+tablename1[5]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[5][j] + ')' 
            muksql = "select count(distinct usn) from "+tablename1[6]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[6][j] + ')' 
        else:
            mptsql = mptsql + " union all " + "select count(distinct usn) from "+tablename1[0]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[0][j] + ')' 
            mkbsql = mkbsql + " union all " + "select count(distinct usn) from "+tablename1[1]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[1][j] + ')' 
            mpssql = mpssql + " union all " + "select count(distinct usn) from "+tablename1[2]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[2][j] + ')' 
            mfrsql = mfrsql + " union all " + "select count(distinct usn) from "+tablename1[3]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[3][j] + ')' 
            mfysql = mfysql + " union all " + "select count(distinct usn) from "+tablename1[4]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[4][j] + ')' 
            mmjsql = mmjsql + " union all " + "select count(distinct usn) from "+tablename1[5]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[5][j] + ')' 
            muksql = muksql + " union all " + "select count(distinct usn) from "+tablename1[6]+ " where keynumber = '1' and usn in " + '(' + stage_csvusn1[6][j] + ')' 
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
        try:
            cur1 = conn.cursor()
            cur1.execute(musql)
            muresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(fgsql)
            fgresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(kbsql)
            kbresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(ptsql)
            ptresult = cur1.fetchall()
            #mptsql,mkbsql,mpssql,mfrsql,mfysql,mmjsql,muksql
            #mptresult,mkbresult,mpsresult,mfrresult,mfyresult,mmjresult,mukresult
            cur1 = conn.cursor()
            cur1.execute(mptsql)
            mptresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(mkbsql)
            mkbresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(mpssql)
            mpsresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(mfrsql)
            mfrresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(mfysql)
            mfyresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(mmjsql)
            mmjresult = cur1.fetchall()

            cur1 = conn.cursor()
            cur1.execute(muksql)
            mukresult = cur1.fetchall()
            return muresult,fgresult,kbresult,ptresult,mptresult,mkbresult,mpsresult,mfrresult,mfyresult,mmjresult,mukresult
        except Exception as e:
            print(e)
        conn.close()

def getresult1():
    lines = ['TB1-1FT-03','TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
    lines1 = ['TB1-1FT-01','TB1-1FT-02']
    tablename = ['allie.aoi_mb','allie.aoi_top','allie.aoi_bot','allie.aoi_ht']
    tablename1 = ['allie.aoi_ht','allie.aoi_KB','allie.aoi_psa','allie.aoi_ahs','allie.aoi_bf','allie.aoi_muk','allie.aoi_muk_psa']
    stage_csvusn = getbystageusn(lines)
    stage_csvusn1 = getbystageusn1(lines1)
    
    csv_result = checkkuducsv(tablename,stage_csvusn,lines,tablename1,stage_csvusn1,lines1)     
    
    return csv_result

def get_result():
    result = getresult1()
    csvlist = [[],[],[],[],[],[],[],[],[],[],[]]
    csvall = [[],[],[],[],[],[],[],[],[],[],[]]
    # for i in range(len(result)):
    #     if i < 1:
    #         for j in range(len(result[i])):
    #             for k in range(len(result[i][j])):
    #                 csvlist[j].append(result[i][j][k])
    #     else:
    #         for j in range(len(result[i])):
    #             for k in range(len(result[i][j])):
    #                 jpglist[j].append(result[i][j][k])
    for j in range(len(result)):
        for k in range(len(result[j])):
            csvlist[j].append(result[j][k])
        
    for i in range(len(csvlist)):
        for j in range(len(csvlist[i])):
            csv1 = list(csvlist[i][j])
            csvall[i].append(csv1)
    
    return csvall

def get_bigdataresult():
    lines = ['TB1-1FT-03','TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
    lines1 = ['TB1-1FT-01','TB1-1FT-02']
    csvstage = ['MU','FG','KB','PT','PT','KB','FO','FR','FY','MJ','UK']
    result = get_result()
    csvresult = []

    for i in range(len(result)):
        if i < 4:
            for j in range(len(result[i])):
                csv1 = ['','','']
                csv1[0] = lines[j]
                csv1[1] = csvstage[i]
                csv1[2] = str(result[i][j][0])
                csvresult.append(csv1)
        else:
            for j in range(len(result[i])):
                csv2 = ['','','']
                csv2[0] = lines1[j]
                csv2[1] = csvstage[i]
                csv2[2] = str(result[i][j][0])
                csvresult.append(csv2)    
    return csvresult

def updatemonthly(inputall,uploadcsvall):
    # rec_dat1 = datetime.datetime.now().strftime('%Y%m%d')
    # rec_dat = str(int(rec_dat1) - 2)
    rec_dat1 = (date.today() + timedelta(days = -2)).strftime("%Y%m%d")
    rec_dat = rec_dat1
    rate = str(float(uploadcsvall)/float(inputall)*100)[0:5]+'%'
    #connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.38:1522/P8MICQ')
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')

    sql2 = 'SELECT count(*) FROM TEMP_UPLOADRATE_MONTHLY where createdate = to_date(' + rec_dat + ',\'YYYYMMDD\')' 
    cursor2 = connection.cursor ()
    cursor2.execute(sql2)
    i = cursor2.fetchall()[0][0]
    if i > 0:
        cursor = connection.cursor()
        sql = '''UPDATE TEMP_UPLOADRATE_MONTHLY SET csvinputusn = :1,uploadcsv= :2,uploadcsvrate= :3 
                    WHERE createdate = to_date(:4,'YYYYMMDD')'''
        cursor.execute(sql,(str(inputall),str(uploadcsvall),rate,rec_dat))
        connection.commit()
    else:
        cursor = connection.cursor()
        sql = '''insert into TEMP_UPLOADRATE_MONTHLY(createdate,csvinputusn,uploadcsv,uploadcsvrate) 
                values(to_date(:1,'YYYYMMDD'),:2,:3,:4)'''
        cursor.execute(sql,(rec_dat,str(inputall),str(uploadcsvall),rate))
        connection.commit()
    connection.close()

if __name__ == "__main__":
    kuduresult = get_bigdataresult()
    inputresult = getmessage()
    csvkuduresult = kuduresult
 
    csvall = []
    lineall = []
    for i in range(len(inputresult)):
        for j in range(len(csvkuduresult)):
            if (csvkuduresult[j][0] == inputresult[i][0]) & (csvkuduresult[j][1] == inputresult[i][1]):
                csv1 = ['','','','','']
                csv1[0] = inputresult[i][0]
                csv1[1] = inputresult[i][1]
                csv1[2] = int(inputresult[i][2])
                csv1[3] = int(csvkuduresult[j][2])
                csv1[4] = str(format(csv1[3]/csv1[2]*100,'.2f')) + '%'
                csvall.append(csv1)
                continue
    
    #print(csvall)
    line = ['TB1-1FT-01','TB1-1FT-02','TB1-1FT-03','TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
    for i in range(len(line)):
        for j in range(len(csvall)):
            if csvall[j][0] == line[i]:
                lineall1 = csvall[j]
                lineall.append(lineall1)
    #print(lineall)   
    inputall=0
    uploadcsvall=0 
    for i in range(len(lineall)):
        inputall += int(lineall[i][2])
        uploadcsvall += int(lineall[i][3])
    updatemonthly(inputall,uploadcsvall)

    