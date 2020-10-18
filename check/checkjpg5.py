import os,time,happybase,datetime,shutil,re
from hdfs import InsecureClient
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import cx_Oracle
import sys
reload(sys)
sys.setdefaultencoding('utf-8')



def send_email(body):
	host = 'wzsowa.wistron.com'
	port = '25'
	sender = 'Robbin_Cai@wistron.com'
	receiver = 'Robbin_Cai@wistron.com'


	msg  = MIMEText(body,'plain','utf-8')
	msg['subject'] = 'mail test'
	msg['from'] = sender
	msg['to'] = receiver

	try:
		s = smtplib.SMTP(host,port)
		s.sendmail(sender,receiver,msg.as_string())
		print('ok')
	except smtplib.SMTPException:
		print('false')


def getjpg():
	connection = cx_Oracle.connect('SFCFA139', 'SFCFA139', '10.41.129.33:1521/PLANT8')
	sql = 'SELECT * FROM V_AOIJPG_NAME ORDER BY renamefilename'
	#sql = sql.format(sn)
	cursor = connection.cursor ()
	cursor.execute (sql)
	jpg = cursor.fetchall()
	return jpg

line_count_ok = [0,0,0,0,0,0,0,0,0,0,0,0]
line_count_false = [0,0,0,0,0,0,0,0,0,0,0,0]
lines = ['TB1-1FT-01','TB1-1FT-02','TB1-1FT-04','TB1-1FT-05','TB1-1FT-06','TB1-1FT-10','TB1-1FT-11','TB1-1FT-12','TB1-1FT-13','TB1-1FT-14','TB1-1FT-15','TB1-1FT-16']
jpg = getjpg()
newjpgname = []

def checkjpg():
	data1 = datetime.datetime.now().strftime('%Y%m%d')
	data2 = str(int(data1)-1)
	os.system('/home/hadoop/wistron-hadoop/hadoop-2.7.1/bin/hdfs fsck /P8AOI/MapData/'+ data1 + ' -files -blocks > /home/hadoop/program/compare_sn/hdfs_log/hdfs_'+ data1 +'.log')
	#os.system('/home/hadoop/wistron-hadoop/hadoop-2.7.1/bin/hdfs fsck /P8AOI/MapData/'+ data2 + ' -files -blocks > /home/hadoop/program/compare_sn/hdfs_log/hdfs_'+ data2 +'.log')
	hdfs_log1 = open('/home/hadoop/program/compare_sn/hdfs_log/hdfs_'+ data1 +'.log').read()
	#hdfs_log2 = open('/home/hadoop/program/compare_sn/hdfs_log/hdfs_'+ data2 +'.log').read()
	#hdfs_log = hdfs_log1 + hdfs_log2
	jpgnames = re.findall(r"/[\S]+.JPG",hdfs_log1)
	for i in range(len(jpgnames)):
		obj = jpgnames[i][24:]
		newjpgname.append(obj)
	#print(len(newjpgname))
	newjpgname1 = sorted(newjpgname)
	for i in range(len(jpg)):
		jpgname = jpg[i][3]
		line    = jpg[i][0]
		for j in range(len(lines)):
			if line == lines[j]:
				if jpgname in newjpgname1:
					#print 'upload fail jpg:',jpgname
					line_count_ok[j] = line_count_ok[j] + 1
					newjpgname1.remove(jpgname)
				else:
					line_count_false[j] = line_count_false[j] + 1

if __name__ == "__main__":
	checkjpg()
	body = 'upload SFTP jpg count is:' + str(len(jpg)) + '\n'
	for j in range(len(lines)):
		body = body + lines[j]+'upload hdfs jpg OK count is:'+str(line_count_ok[j]) + '\n' + lines[j]+'upload hdfs jpg NG count is:'+str(line_count_false[j]) + '\n'
	print type(body)
	send_email(body)