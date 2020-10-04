import os,time,happybase,datetime,shutil,re
from hdfs import InsecureClient
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from impala.dbapi import connect
import cx_Oracle
import sys

def send_email(body):
    #设置默认字符集为UTF8 不然有些时候转码会出问题
    default_encoding = 'utf-8'
    #发送邮件的相关信息，根据你实际情况填写
    smtpHost = 'wzsowa.wistron.com'
    smtpPort = '587'
    sslPort  = '587'
    fromMail = 'Robbin_Cai@wistron.com'
    #toMail   = 'Robbin_Cai@wistron.com,MZL810.WZS.Wistron@wistron.com,MZL820.WZS.Wistron@wistron.com,Wave_Lin@wistron.com'
    toMail   = 'Robbin_Cai@wistron.com,Nander_Huang@wistron.com'
    username = 'Z19033018'
    password = '19033018C...'
    #邮件标题和内容
    subject  = u'UPLOAD AOI CSV&JPG REPORT'
    #初始化邮件
    encoding = 'utf-8'
    #mail = MIMEText(body.encode(encoding),'plain',encoding)
    mail = MIMEText(body.encode(encoding),'html',encoding)
    mail['Subject'] = Header(subject,encoding)
    mail['From'] = fromMail#发送者
    #mail['To'] = toMail#接收者
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
    # sql1 = 'SELECT line,stage,COUNT(usn) FROM V_AOICSV_NAME1 GROUP BY line,stage ORDER BY stage,line'
    # sql2 = 'SELECT line,stage,COUNT(usn) FROM V_AOICSV_NAME2 GROUP BY line,stage ORDER BY stage,line'
    # sql3 = 'SELECT line,stage,COUNT(DISTINCT jpgname1) FROM V_AOIJPG_NAME GROUP BY line,stage ORDER BY stage,line'
    sql = 'SELECT line,stage,COUNT(usn) FROM V_AOI_FILECOUNT GROUP BY line,stage ORDER BY stage,line'
    # cursor1 = connection.cursor ()
    # cursor1.execute (sql1)
    # csv1 = cursor1.fetchall()
    # cursor2 = connection.cursor ()
    # cursor2.execute (sql2)
    # csv2 = cursor2.fetchall()
    # cursor3 = connection.cursor ()
    # cursor3.execute (sql3)
    # jpg1 = cursor3.fetchall()
    cursor = connection.cursor ()
    cursor.execute (sql)
    inputresult = cursor.fetchall()
    return inputresult

def getbystageusn(lines):
    mu_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    fg_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    kb_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    pt_csvname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
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
                elif csvmessage[i][1] == 'PS':
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
    #pt,kb,ps,fr,fy,mj,uk
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
    #pt,kb,ps,fr,fy,mj,uk
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

def getjpg():
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql = 'SELECT * FROM V_AOIJPG_NAME'
    cursor1 = connection.cursor ()
    cursor1.execute (sql)
    jpg = cursor1.fetchall()
    return jpg
#返回MU,FG,KB,PT,BT,FU各站的所有文件名，返回类型：字符串
def getbystagejpg(lines):
    #MU,FG,KB,PT,BT,FU
    mu_jpgname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    fg_jpgname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    kb_jpgname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    pt_jpgname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    bt_jpgname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    fu_jpgname = ['\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'','\' \'']
    jpgmessage = getjpg()
    for i in range(len(jpgmessage)):
        for j in range(len(lines)):
            if jpgmessage[i][0] == lines[j]:
                if jpgmessage[i][1] == 'MU':
                    mu_jpgname1 = jpgmessage[i][3]
                    mu_jpgname2 = jpgmessage[i][4]
                    if mu_jpgname[j] == '\' \'':
                        mu_jpgname[j] = '\'' + mu_jpgname1 + '\'' #+ ',\'' + mu_jpgname2 + '\''
                    else:
                        mu_jpgname[j] = mu_jpgname[j] + ',\'' + mu_jpgname1 + '\'' #+ ',\'' + mu_jpgname2 + '\''
                elif jpgmessage[i][1] == 'FG':
                    fg_jpgname1 = jpgmessage[i][3]
                    fg_jpgname2 = jpgmessage[i][4]
                    if fg_jpgname[j] == '\' \'':
                        fg_jpgname[j] = '\'' + fg_jpgname1 + '\'' #+ ',\'' + fg_jpgname2 + '\''
                    else:
                        fg_jpgname[j] = fg_jpgname[j] + ',\'' + fg_jpgname1 + '\'' #+ ',\'' + fg_jpgname2 + '\''
                elif jpgmessage[i][1] == 'KB':
                    kb_jpgname1 = jpgmessage[i][3]
                    kb_jpgname2 = jpgmessage[i][4]
                    if kb_jpgname[j] == '\' \'':
                        kb_jpgname[j] = '\'' + kb_jpgname1 + '\'' #+ ',\'' + kb_jpgname2 + '\''
                    else:
                        kb_jpgname[j] = kb_jpgname[j] + ',\'' + kb_jpgname1 + '\'' #+ ',\'' + kb_jpgname2 + '\''
                elif jpgmessage[i][1] == 'PT':
                    pt_jpgname1 = jpgmessage[i][3]
                    pt_jpgname2 = jpgmessage[i][4]
                    if pt_jpgname[j] == '\' \'':
                        pt_jpgname[j] = '\'' + pt_jpgname1 + '\'' #+ ',\'' + pt_jpgname2 + '\''
                    else:
                        pt_jpgname[j] = pt_jpgname[j] + ',\'' + pt_jpgname1 + '\'' #+ ',\'' + pt_jpgname2 + '\''
                elif jpgmessage[i][1] == 'BT':
                    bt_jpgname1 = jpgmessage[i][3]
                    bt_jpgname2 = jpgmessage[i][4]
                    if bt_jpgname[j] == '\' \'':
                        bt_jpgname[j] = '\'' + bt_jpgname1 + '\'' #+ ',\'' + bt_jpgname2 + '\''
                    else:
                        bt_jpgname[j] = bt_jpgname[j] + ',\'' + bt_jpgname1 + '\'' #+ ',\'' + bt_jpgname2 + '\''
                elif jpgmessage[i][1] == 'FU':
                    fu_jpgname1 = jpgmessage[i][3]
                    fu_jpgname2 = jpgmessage[i][4]
                    if fu_jpgname[j] == '\' \'':
                        fu_jpgname[j] = '\'' + fu_jpgname1 + '\'' #+ ',\'' + fu_jpgname2 + '\''
                    else:
                        fu_jpgname[j] = fu_jpgname[j] + ',\'' + fu_jpgname1 + '\'' #+ ',\'' + fu_jpgname2 + '\''
            else:pass
    return mu_jpgname,fg_jpgname,kb_jpgname,pt_jpgname,bt_jpgname,fu_jpgname
#返回pt,kb,ps,fr,fy,mj,uk各站的所有文件名，返回类型：字符串
def getbystagejpg1(lines1):
    #pt,kb,ps,fr,fy,mj,uk
    pt_jpgname = ['\' \'','\' \'']
    kb_jpgname = ['\' \'','\' \'']
    ps_jpgname = ['\' \'','\' \'']
    fr_jpgname = ['\' \'','\' \'']
    fy_jpgname = ['\' \'','\' \'']
    mj_jpgname = ['\' \'','\' \'']
    uk_jpgname = ['\' \'','\' \'']
    jpgmessage = getjpg()
    for i in range(len(jpgmessage)):
        for j in range(len(lines1)):
            if jpgmessage[i][0] == lines1[j]:
                if jpgmessage[i][1] == 'PT':
                    pt_jpgname1 = jpgmessage[i][3]
                    pt_jpgname2 = jpgmessage[i][4]
                    if pt_jpgname[j] == '\' \'':
                        pt_jpgname[j] = '\'' + pt_jpgname1 + '\'' #+ ',\'' + pt_jpgname2 + '\''
                    else:
                        pt_jpgname[j] = pt_jpgname[j] + ',\'' + pt_jpgname1 + '\'' #+ ',\'' + pt_jpgname2 + '\''
                elif jpgmessage[i][1] == 'KB':
                    kb_jpgname1 = jpgmessage[i][3]
                    kb_jpgname2 = jpgmessage[i][4]
                    if kb_jpgname[j] == '\' \'':
                        kb_jpgname[j] = '\'' + kb_jpgname1 + '\'' #+ ',\'' + kb_jpgname2 + '\''
                    else:
                        kb_jpgname[j] = kb_jpgname[j] + ',\'' + kb_jpgname1 + '\'' #+ ',\'' + kb_jpgname2 + '\''
                elif jpgmessage[i][1] == 'PS':
                    ps_jpgname1 = jpgmessage[i][3]
                    ps_jpgname2 = jpgmessage[i][4]
                    if ps_jpgname[j] == '\' \'':
                        ps_jpgname[j] = '\'' + ps_jpgname1 + '\'' #+ ',\'' + ps_jpgname2 + '\''
                    else:
                        ps_jpgname[j] = ps_jpgname[j] + ',\'' + ps_jpgname1 + '\'' #+ ',\'' + ps_jpgname2 + '\''
                elif jpgmessage[i][1] == 'FR':
                    fr_jpgname1 = jpgmessage[i][3]
                    fr_jpgname2 = jpgmessage[i][4]
                    if fr_jpgname[j] == '\' \'':
                        fr_jpgname[j] = '\'' + fr_jpgname1 + '\'' #+ ',\'' + fr_jpgname2 + '\''
                    else:
                        fr_jpgname[j] = fr_jpgname[j] + ',\'' + fr_jpgname1 + '\'' #+ ',\'' + fr_jpgname2 + '\''
                elif jpgmessage[i][1] == 'FY':
                    fy_jpgname1 = jpgmessage[i][3]
                    fy_jpgname2 = jpgmessage[i][4]
                    if fy_jpgname[j] == '\' \'':
                        fy_jpgname[j] = '\'' + fy_jpgname1 + '\'' #+ ',\'' + fy_jpgname2 + '\''
                    else:
                        fy_jpgname[j] = fy_jpgname[j] + ',\'' + fy_jpgname1 + '\'' #+ ',\'' + fy_jpgname2 + '\''
                elif jpgmessage[i][1] == 'MJ':
                    mj_jpgname1 = jpgmessage[i][3]
                    mj_jpgname2 = jpgmessage[i][4]
                    if mj_jpgname[j] == '\' \'':
                        mj_jpgname[j] = '\'' + mj_jpgname1 + '\'' #+ ',\'' + mj_jpgname2 + '\''
                    else:
                        mj_jpgname[j] = mj_jpgname[j] + ',\'' + mj_jpgname1 + '\'' #+ ',\'' + mj_jpgname2 + '\''
                elif jpgmessage[i][1] == 'UK':
                    uk_jpgname1 = jpgmessage[i][3]
                    uk_jpgname2 = jpgmessage[i][4]
                    if uk_jpgname[j] == '\' \'':
                        uk_jpgname[j] = '\'' + uk_jpgname1 + '\'' #+ ',\'' + uk_jpgname2 + '\''
                    else:
                        uk_jpgname[j] = uk_jpgname[j] + ',\'' + uk_jpgname1 + '\'' #+ ',\'' + uk_jpgname2 + '\''
            else:pass
    return pt_jpgname,kb_jpgname,ps_jpgname,fr_jpgname,fy_jpgname,mj_jpgname,uk_jpgname
#result1为新机种MU,FG,KB,PT,BT,FU在lines对应的数据
#result1为新机种pt,kb,ps,fr,fy,mj,uk在lines1对应的数据
def checkkudujpg(jpgtablename,stage_jpgname,lines,stage_jpgname1,lines1):
    hostname1 = 'p8cdhdatap01.wzs.wistron'
    hostname2 = 'p8cdhdatap02.wzs.wistron'
    hostname3 = 'p8cdhdatap03.wzs.wistron'
    port = 21050
    sql1 = ''
    sql2 = ''
    for i in range(len(stage_jpgname)):
        for j in range(len(lines)):
            if sql1 == '':
                sql1 = "SELECT count(distinct usn) from " + jpgtablename + " where imageurl in " +  '(' + stage_jpgname[i][j] + ')'
            else:
                sql1 = sql1 + " union all " + "SELECT count(distinct usn) from " + jpgtablename + " where imageurl in " +  '(' + stage_jpgname[i][j] + ')'
    for i in range(len(stage_jpgname1)):
        for j in range(len(lines1)):
            if sql2 == '':
                sql2 = "SELECT count(distinct usn) from " + jpgtablename + " where imageurl in " +  '(' + stage_jpgname1[i][j] + ')'
            else:
                sql2 = sql2 + " union all " + "SELECT count(distinct usn) from " + jpgtablename + " where imageurl in " +  '(' + stage_jpgname1[i][j] + ')'
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
            cur1=conn.cursor()
            cur1.execute(sql1)
            result1 = cur1.fetchall()
            cur1=conn.cursor()
            cur1.execute(sql2)
            result2 = cur1.fetchall()
            return result1,result2
        except Exception as e:
            print(e)
        conn.close()

def getresult1():
    lines = ['TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
    lines1 = ['TB1-1FT-01','TB1-1FT-02']
    jpgtablename = 'allie.aoi_imageurl'
    tablename = ['allie.aoi_mb','allie.aoi_top','allie.aoi_bot','allie.aoi_ht']
    tablename1 = ['allie.aoi_ht','allie.aoi_KB','allie.aoi_psa','allie.aoi_ahs','allie.aoi_bf','allie.aoi_muk','allie.aoi_muk_psa']
    stage_csvusn = getbystageusn(lines)
    stage_csvusn1 = getbystageusn1(lines1)
    stage_jpgname = getbystagejpg(lines)
    stage_jpgname1 = getbystagejpg1(lines1)
    csv_result = checkkuducsv(tablename,stage_csvusn,lines,tablename1,stage_csvusn1,lines1)     
    jpg_result = checkkudujpg(jpgtablename,stage_jpgname,lines,stage_jpgname1,lines1)
    return csv_result,jpg_result

def get_result():
    result = getresult1()
    # for i in range(len(result)):
    #     #resulu[0]为csv信息，result[0][0]为MU站信息，result[0][1]为FG站信息,result[0][2]为KB站信息,result[0][3]为pt站信息 #pt,kb,ps,fr,fy,mj,uk
    #     #result[1]为jpg信息，result[1][0]为新机种信息MU,FG,KB,PT,BT,FU，result[1][1]为旧机种信息 #pt,kb,ps,fr,fy,mj,uk
    #     print(result[i])
    # print('CSVMU:',result[0][0])
    # print('CSVFG:',result[0][1])
    # print('CSVKB:',result[0][2])
    # print('CSVPT:',result[0][3])
    # print('CSVPT:',result[0][4])
    # print('CSVKB:',result[0][5])
    # print('CSVPS:',result[0][6])
    # print('CSVFR:',result[0][7])
    # print('CSVFY:',result[0][8])
    # print('CSVMJ:',result[0][9])
    # print('CSVUK:',result[0][10])
    # print('RJPG:',result[1][0])
    # print('MJPG:',result[1][1])
    csvlist = [[],[],[],[],[],[],[],[],[],[],[]]
    jpglist = [[],[]]
    csvall = [[],[],[],[],[],[],[],[],[],[],[]]
    jpgall = [[],[]]
    for i in range(len(result)):
        if i < 1:
            for j in range(len(result[i])):
                for k in range(len(result[i][j])):
                    csvlist[j].append(result[i][j][k])
        else:
            for j in range(len(result[i])):
                for k in range(len(result[i][j])):
                    jpglist[j].append(result[i][j][k])
    for i in range(len(csvlist)):
        for j in range(len(csvlist[i])):
            csv1 = list(csvlist[i][j])
            csvall[i].append(csv1)
    for i in range(len(jpglist)):
        for j in range(len(jpglist[i])):
            jpg1 = list(jpglist[i][j])
            jpgall[i].append(jpg1)
    return csvall,jpgall

def get_bigdataresult():
    lines = ['TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
    lines1 = ['TB1-1FT-01','TB1-1FT-02']
    csvstage = ['MU','FG','KB','PT','PT','KB','PS','FR','FY','MJ','UK']
    jpgstage = ['MU','FG','KB','PT','BT','FU','PT','KB','PS','FR','FY','MJ','UK']
    result = get_result()
    csvresult = []
    jpgresult = []
    for i in range(len(result[0])):
        if i < 4:
            for j in range(len(result[0][i])):
                csv1 = ['','','']
                #print("csv:",lines[j],csvstage[i],result[0][i][j])
                #csv1 = '['+lines[j]+','+csvstage[i]+','+str(result[0][i][j][0])+']' 
                csv1[0] = lines[j]
                csv1[1] = csvstage[i]
                csv1[2] = str(result[0][i][j][0])
                csvresult.append(csv1)
        else:
            for j in range(len(result[0][i])):
                csv2 = ['','','']
                #print("csv:",lines1[j],csvstage[i],result[0][i][j])
                #csv2 = '['+lines1[j]+','+csvstage[i]+','+str(result[0][i][j][0])+']' 
                csv2[0] = lines1[j]
                csv2[1] = csvstage[i]
                csv2[2] = str(result[0][i][j][0])
                csvresult.append(csv2)
    for i in range(len(result[1])):
        if i == 0:
            for j in range(int(len(result[1][i])/len(lines))):
                for k in range(len(lines)):
                    jpg1 = ['','','']
                    #print("jpg:",lines[k],jpgstage[j],result[1][i][10*j+k])
                    jpg1[0] = lines[k]
                    jpg1[1] = jpgstage[j]
                    jpg1[2] = str(result[1][i][10*j+k][0])
                    jpgresult.append(jpg1)
        else:
            for j in range(int(len(result[1][i])/len(lines1))):
                for k in range(len(lines1)):
                    jpg2 = ['','','']
                    #print("jpg:",lines1[k],jpgstage[j+6],result[1][i][2*j+k])
                    jpg2[0] = lines1[k]
                    jpg2[1] = jpgstage[j+6]
                    jpg2[2] = str(result[1][i][2*j+k][0])
                    jpgresult.append(jpg2)
    return csvresult,jpgresult
if __name__ == "__main__":
    kuduresult = get_bigdataresult()
    inputresult = getmessage()
    csvkuduresult = kuduresult[0]
    jpgkuduresult = kuduresult[1]
    # csvinputresult = inputresult[0] + inputresult[1]
    # jpginputresult = inputresult[2]   
    csvall = []
    jpgall = []
    all = []
    lineall = []
    for i in range(len(inputresult)):
        for j in range(len(csvkuduresult)):
            if (csvkuduresult[j][0] == inputresult[i][0]) & (csvkuduresult[j][1] == inputresult[i][1]):
                csv1 = ['','','','']
                csv1[0] = inputresult[i][0]
                csv1[1] = inputresult[i][1]
                csv1[2] = inputresult[i][2]
                csv1[3] = csvkuduresult[j][2]
                csvall.append(csv1)
                continue
    #print("csv:",csvall)
    for i in range(len(inputresult)):
        for j in range(len(jpgkuduresult)):
            if (jpgkuduresult[j][0] == inputresult[i][0]) & (jpgkuduresult[j][1] == inputresult[i][1]):
                jpg1 = ['','','','']
                jpg1[0] = inputresult[i][0]
                jpg1[1] = inputresult[i][1]
                jpg1[2] = inputresult[i][2]
                jpg1[3] = jpgkuduresult[j][2]
                jpgall.append(jpg1)
                continue   
    for i in range(len(jpgall)):
        for j in range(len(csvall)):
            if (csvall[j][0] == jpgall[i][0]) and (csvall[j][1] == jpgall[i][1]):
                all1 = ['','','','','','','']
                all1[0] = jpgall[i][0]
                all1[1] = jpgall[i][1]
                all1[2] = jpgall[i][2]
                all1[3] = int(jpgall[i][3])
                all1[4] = int(csvall[j][3])
                all1[5] = str(format(all1[3]/all1[2]*100,'.1f')) + '%'
                all1[6] = str(format(all1[4]/all1[2]*100,'.1f')) + '%'
                all.append(all1)
                continue
        if jpgall[i][1] == 'BT' or jpgall[i][1] == 'FU':
            all2 = ['','','','','','','']
            all2[0] = jpgall[i][0]
            all2[1] = jpgall[i][1]
            all2[2] = jpgall[i][2]
            all2[3] = int(jpgall[i][3])
            all2[4] = 0
            all2[5] = str(format(all2[3]/all2[2]*100,'.1f')) + '%'
            all2[6] = 0
            all.append(all2)
    line = ['TB1-1FT-01','TB1-1FT-02','TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
    for i in range(len(line)):
        for j in range(len(all)):
            if all[j][0] == line[i]:
                lineall1 = all[j]
                lineall.append(lineall1)
    str1 = '''<h1>Dear All：以下为昨日8点到今日8点AOI 机台文件从cilent端到大数据平台的上传情况</h1><table border="1" bgcolor="#F0FFFF"> 
                        <tr>
                            <th>LINE</th>
                            <th>STAGE</th>
                            <th>INPUTUSN</th>
                            <th>JPG_HDFS</th>
                            <th>CSV_KUDU</th>
                            <th>JPG_RATE</th>
                            <th>CSV_RATE</th>
                        </tr>'''
    for i in range(len(lineall)):
        str1 = str1 + '<tr>'
        for j in range(len(lineall[i])):
            str1 = str1 + '<th>' + str(lineall[i][j]) + '</th>'
        str1 = str1 + '</tr>'
    str1 = str1 + '</table>'
    send_email(str1)

    #print(all)  
    #print(lineall)