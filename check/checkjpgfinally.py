import os,time,happybase,datetime,shutil,re
from hdfs import InsecureClient
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from datetime import date, timedelta
import cx_Oracle
import sys

def getjpg():
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql = 'SELECT * FROM V_AOIJPG_NAME'
    cursor1 = connection.cursor ()
    cursor1.execute (sql)
    jpg = cursor1.fetchall()
    return jpg

def getinput():
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql = 'SELECT COUNT(*) FROM V_AOI_FILECOUNT'
    cursor1 = connection.cursor ()
    cursor1.execute (sql)
    count = cursor1.fetchall()
    return count

# def gethdfs_log():
#     rec_dat1 = (date.today() + timedelta(days = -2)).strftime("%m%d")
#     rec_dat = rec_dat1
#     # rec_dat1 = datetime.datetime.now().strftime('%m%d')
#     # rec_dat = str(int(rec_dat1) - 2)
#     path = r'C:\Users\Z19033018.WZSCN\Desktop\hdfs_log'
#     path = path + '\\' +rec_dat
#     model = os.listdir(path)
#     hdfs_log = ''
#     for x in range(len(model)):
#         hdfs_log1 = open(path+'\\'+model[x]).read()
#         hdfs_log = hdfs_log + hdfs_log1
#     return hdfs_log

def gethdfs_log():
    rec_dat = (date.today() + timedelta(days = -2)).strftime("%Y%m%d")
    path = r'/home/hadoop/program/compare_sn/hdfs_log/'
    model = os.listdir(path)
    hdfs_log = ''
    for i in range(len(model)):
        if rec_dat in model[i]:
            path1 = path + model[i]
            hdfs_log1 = open(path1).read()
            hdfs_log = hdfs_log + hdfs_log1
    return hdfs_log

def checkjpg():
	count=0
	jpgcount = []
	jpg=getjpg()
	hdfs_log1 = gethdfs_log()	
	jpgnames = re.findall(r"/[\S]+.JPG",hdfs_log1)
	for i in range(len(jpg)):
		if jpg[i][0]+jpg[i][1] not in jpgcount:
			if jpg[i][2] in jpgnames:
				jpgcount1 = jpg[i][0]+jpg[i][1]
				jpgcount.append(jpgcount1)
				count = count + 1
				print count	
	return count

def updatemonthly(jpginput,jpgupload):
    rec_dat1 = (date.today() + timedelta(days = -2)).strftime("%Y%m%d")
    rec_dat = rec_dat1
    # rec_dat1 = datetime.datetime.now().strftime('%Y%m%d')
    # rec_dat = str(int(rec_dat1) - 2)
    rate = str(float(jpgupload)/float(jpginput)*100)[0:5]+'%'
    #connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.38:1522/P8MICQ')
    connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
    sql2 = 'SELECT count(*) FROM TEMP_UPLOADRATE_MONTHLY where createdate = to_date(' + rec_dat + ',\'YYYYMMDD\')' 
    cursor2 = connection.cursor ()
    cursor2.execute(sql2)
    i = cursor2.fetchall()[0][0]
    if i > 0:
        cursor = connection.cursor()
        sql = '''UPDATE TEMP_UPLOADRATE_MONTHLY SET jpginputusn = :1,uploadjpg= :2,uploadjpgrate= :3 
                    WHERE createdate = to_date(:4,'YYYYMMDD')'''
        cursor.execute(sql,(str(jpginput),str(jpgupload),rate,rec_dat))
        connection.commit()
    else:
        cursor = connection.cursor()
        sql = '''insert into TEMP_UPLOADRATE_MONTHLY(createdate,jpginputusn,uploadjpg,uploadjpgrate) 
                values(to_date(:1,'YYYYMMDD'),:2,:3,:4)'''
        cursor.execute(sql,(rec_dat,str(jpginput),str(jpgupload),rate))
        connection.commit()
    connection.close()

jpginput  = getinput()[0][0]
jpgupload = checkjpg()
print(jpginput,jpgupload)
updatemonthly(jpginput,jpgupload)



