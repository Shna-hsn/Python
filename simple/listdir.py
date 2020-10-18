import os

path = r'C:/Users/Z18073047/Desktop/data/Python/program/other/file/upload'
fileslist  = os.listdir(path)
# for i in range(len(fileslist)):
#   USN = fileslist[i].split('-')[1].split('_')[0]
#   print(USN)
# print(i)
for filename in fileslist:
  print(filename)