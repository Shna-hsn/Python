import os,datetime,time,shutil
import random

start_time = time.time()
if __name__  == '__main__':
	ai_jpg_path = '/data/sftp/ai_jpg/upload'
	tmp_jpg_path = '/data/buffer/ai_jpg_tmp'


	ai_file_list = [a for a in os.listdir(ai_jpg_path) if '.jpg' in a]
	time.sleep(10)
	ai_list_len = 0
	if len(ai_file_list) > 10000:
		ai_list_len = 10000
		n=ai_list_len
	else:
		ai_list_len = len(ai_file_list)
		n=ai_list_len
	for i in range(ai_list_len):
		ai_jpg = ai_file_list[i]
		ok_jpg = ai_jpg
		foldernum = str(random.randint(1,5))
		shutil.move(ai_jpg_path+os.sep+ai_jpg,tmp_jpg_path+os.sep+foldernum+os.sep+ok_jpg)
		#print i+1,"/",n,"moving",ai_jpg
		print(i+1,"/",n,"moving",ai_jpg)
end_time = time.time()
com_dat  = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
# print "Job: 01jpg_move_data_buffer_ai_jpg_tmp_v1"
# print "Total file count:" ,n
# print "Total time:		",end_time - start_time,"s"
# print "Speed: ",n / (end_time - start_time),"pcs/s,at data/time: ",com_dat
# print("Job: 01jpg_move_data_buffer_ai_jpg_tmp_v1")
# print("Total file count:" ,n)
# print("Total time:		",end_time - start_time,"s")
# print("Speed: ",n / (end_time - start_time),"pcs/s,at data/time: ",com_dat)