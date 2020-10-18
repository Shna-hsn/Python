import os,time,happybase,datetime,shutil,re
from hdfs import InsecureClient
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from impala.dbapi import connect
import cx_Oracle
import sys

def getHBase():
    connection = happybase.Connection('10.41.158.65')
    table = connection.table('p8_aoi_csv')
    row = table.row("FPW0425G169Q1GQ1M")
    for item in row.items():
        print(item[0])
getHBase()