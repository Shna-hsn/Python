# -*- coding: utf-8 -*-
"""
Created on Wed May 04 08:02:32 2016

@author: 200511034
"""

import paramiko,base64,os,sys,time,socket
def ssh_connect( _host, _username, _password ):
    try:
        _ssh_fd = paramiko.SSHClient()
        _ssh_fd.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
        _ssh_fd.connect( _host, username = _username, password = _password )
    except Exception e:
        print( 'ssh %s@%s: %s' % (_username, _host, e) )
        exit()
    return _ssh_fd
	
def sftp_open( _ssh_fd ):
    return _ssh_fd.open_sftp()
	
def sftp_put( _sftp_fd, _put_from_path, _put_to_path ):
    return _sftp_fd.put( _put_from_path, _put_to_path )
	
def sftp_close( _sftp_fd ):
    _sftp_fd.close()
	
def ssh_close( _ssh_fd ):
    _ssh_fd.close()    
 
c=0
session_list=os.popen('tasklist').readlines()
for i in range(len(session_list)):
    if "aoi_sftp" in session_list[i]:
        c+=1
if c>2:
    sys.exit() 
 
conf_path='aoi_sftp_prd.conf'
conf={}
value=[new for new in open(conf_path).readlines()]
for i in range(len(value)):
    temp=value[i].strip().split("=",1)
    conf[temp[0]]=temp[1] 
 
sftp_pwd=conf['sftp_pwd']
sftpip=conf['sftpip']
sftpusr=conf['sftpusr']
sftppwd=base64.b64decode(sftp_pwd)

sshd = ssh_connect( sftpip, sftpusr, sftppwd )
sftpd = sftp_open( sshd )  

modelfolder=[data1 for data1 in os.listdir(conf['FromFolder1'])]	
for a1 in range(len(modelfolder)):
    datefolder=[data2 for data2 in os.listdir(conf['FromFolder1']+os.sep+modelfolder[a1])]
    for a2 in range(len(datefolder)):
        aoifile1=[data3 for data3 in os.listdir(conf['FromFolder1']+os.sep+modelfolder[a1]+os.sep+datefolder[a2]) if "com" not in data3]
        if len(aoifile1) != 0:
            time.sleep(2)
            for a3 in range(len(aoifile1)):
                sftp_put( sftpd, conf['FromFolder1']+os.sep+modelfolder[a1]+os.sep+datefolder[a2]+os.sep+aoifile1[a3], "./"+conf['ToFoloder']+"/"+modelfolder[a1]+"_"+datefolder[a2]+"_"+aoifile1[a3] )
                os.rename(conf['FromFolder1']+os.sep+modelfolder[a1]+os.sep+datefolder[a2]+os.sep+aoifile1[a3],conf['FromFolder1']+os.sep+modelfolder[a1]+os.sep+datefolder[a2]+os.sep+"com"+aoifile1[a3])


host = socket.gethostname()
check_program = "aoi_jpeg_chk"
dt=str(int(time.time()))
alive_check=open("alive_check.log","wb")
alive_check.write(check_program+" alive at "+dt+os.linesep)
alive_check.close()
sftp_put(sftpd, "alive_check.log", "./other/"+host+"_"+check_program+"_alive_check.log")