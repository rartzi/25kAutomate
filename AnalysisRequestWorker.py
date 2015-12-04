#!/usr/logal/bin/python
import sys, getopt
import boto.sqs
import csv
import json 
import os
import time
import timeit
import signal
from signal import  SIGPIPE, SIG_DFL 
import subprocess

from decimal import *
from subprocess import PIPE, Popen 
from boto.sqs.message import Message

########### File system Functions
def get_mount_point(pathname):
    "Get the mount point of the filesystem containing pathname"
    pathname= os.path.normcase(os.path.realpath(pathname))
    parent_device= path_device= os.stat(pathname).st_dev
    while parent_device == path_device:
        mount_point= pathname
        pathname= os.path.dirname(pathname)
        if pathname == mount_point: break
        parent_device= os.stat(pathname).st_dev
    return mount_point

def get_mounted_device(pathname):
    "Get the device mounted at pathname"
    # uses "/proc/mounts"
    pathname= os.path.normcase(pathname) # might be unnecessary here
    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields= line.rstrip('\n').split()
                # note that line above assumes that
                # no mount points contain whitespace
                if fields[1] == pathname:
                    return fields[0]
    except EnvironmentError:
        pass
    return None # explicit

def get_fs_freespace(pathname):
    "Get the free space of the filesystem containing pathname"
    stat= os.statvfs(pathname)
    # use f_bfree for superuser, or f_bavail if filesystem
    # has reserved space for superuser
    return stat.f_bfree*stat.f_bsize


###########
def key_exist(bucket,key):
	# Check id obkect is in S3 Bucket
	# print "looking for s3 key: " , key  ,   " Exist: " , bucket.get_key(key)
        return bucket.get_key(key)

def create_queue(conn , queue_name, visibility_timeout):
	q = '' 
	try:
		print " creating queue : " , queue_name
		q = conn.create_queue(queue_name, visibility_timeout)
	except:
		print 'Issues creating the queue: ' , queue_name
	return q

def get_queue_by_name(conn, queue_name, visibility_timeout):
        q = ''
	print " searching for queue : " , queue_name
        try:
                q = conn.get_queue(queue_name)
		print " Found a Queue : " , q 
		if q:
			return q
		else:
			q = create_queue(conn, queue_name  , visibility_timeout)
			return q
        except:
		print 'Cant  queue: ' , queue_name , " will create one"
                q = create_queue(conn, queue_name )
        return q


def cleanup_files(env , uuid_path):
	return_code = 0
        try:
		# print "Cleanup: ", uuid_path
		# print 'rm -r ' + uuid_path + '*'
		if env == 'cloud':
			os.system('sudo touch ' + uuid_path )
           		os.system('sudo rm -rf ' + uuid_path + '*')
		else:
			os.system('touch ' + uuid_path )
			os.system('rm -rf ' + uuid_path + '*')
	except:
		print 'Issues cleaning up uuid files'
		retunr_code = 999

	return return_code

def push_to_s3(s3_bucket, uuid, filelist):
        return_code = 0
        # print "Push to S3:   uuid:" + uuid + '  filelist: ', filelist 
        s3_urls_list = {}
	file_name = ''
        url_number = 1
	for file in filelist:
                print "pushing:", file
                file_name = os.path.basename(file)
		key = 's3-url-' + str(url_number)
		url_number += 1
                s3cmd = 'aws s3 cp ' + file   + ' ' + 's3://' + s3_bucket + '/' + uuid + '.' + file_name + ' --sse '
                try:
                        # print 'aws command:' , s3cmd
			os.system(s3cmd)
                        # print 'file name : ', file_name , ' index: ' , key
                        s3_urls_list[key] = 'https://s3.amazonaws.com/' + s3_bucket + '/' + uuid + '.' + file_name
		except:
			return_code = 999

        return s3_urls_list

def get_local_dowloaded_files( uuid, filelist):
        return_code = 0
        print "Get Local Files Downloaded:   uuid:" + uuid + '  filelist: ', filelist 
        files_urls_list = {}
        file_name = ''
        url_number = 1
        for file in filelist:
                print "pushing:", file
                file_name = os.path.basename(file)
                key = 'localfile-' + str(url_number)
                url_number += 1
                files_urls_list[key] = file
	print files_urls_list
        return  files_urls_list


def get_filepaths(directory):
    # print 'getfilepaths: ' , directory
    """
    This function will generate the file names in a directory 
    tree by walking the tree either top-down or bottom-up. For each 
    directory in the tree rooted at directory top (including top itself), 
    it yields a 3-tuple (dirpath, dirnames, filenames).
    """
    file_paths = []  # List which will store all of the full filepaths.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            # Join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)  # Add it to the list.

    return file_paths  # Self-explanatory.

def cmdline(command):
    process = Popen(
        args=command,
        stdout=PIPE,
        shell=True
    )
    return process.communicate()[0]

def signal_handler(signal, frame):
    print 'You pressed Ctrl+C!'
    # for p in jobs:
    #     p.terminate()
    sys.exit(0)

def create_dir(path):
	try:
    		os.makedirs(path)
	except:
		print "Error while creating dir: " , path
		pass		


def main(argv):
#
  print 'Number of arguments:', len(sys.argv), 'arguments.'
  print 'Argument List:', str(sys.argv)
  # 
  # Default var set
  #   
  code_version = '1.5' # remember to change on message stracture changes
  #
  max_messages_to_handle = 1
  max_threads_for_gtdownload = 4
  statistics_queue_name = '25kStatisticsQueue'
  queue_name = '25kRequestQueue'
  analysis_queue_name = '25kAnalysisQueue'
  request_bucket = 'az-ngs-25k'
#  request_bucket = 'az-ngs-test25k'
  request_bucket_name = 's3://' + request_bucket
  visibility_timeout = 1800
  environment = 'local'
#
#    Handle Input paramenters : FeedInRequest --qname=Queue --fname=filename with uuids
#
  try:
     opts, args = getopt.getopt(argv,"hq:e",["qname=","env="])
  except getopt.GetoptError:
        print 'myname.py -q <queu_name> -e <cloud>"'
        sys.exit(2)

  for opt, arg in opts:
      print  opt
      if opt == '-h':
         print 'myname.py -q <queue_name> -e <max_messages>'
         sys.exit()
      elif opt in ("-q", "--qname"):
         queue_name = arg
      elif opt in ("-e", "--env"):
          environment = arg

  print "Queue Name ", queue_name
  print "environment: ", environment

#
#   Cloud 
#
#
  if ( environment == 'cloud' ):
	print "Cloud Environment "
	max_messages_to_handle = 1
	statistics_queue_name = '25kStatisticsQueue'
	queue_name = '25kRequestQueue'
	analysis_queue_name = '25kAnalysisQueue'
	request_bucket = 'az-ngs-25k'
	#  request_bucket = 'az-ngs-test25k'
	request_bucket_name = 's3://' + request_bucket
  	visibility_timeout = 1800
	try:
		target_dir_cloud = '/mnt/download'
		log_dir = target_dir_cloud + '/log'
		create_dir(target_dir_cloud)
        	create_dir(log_dir)
	except:
		print " Folders : " , target_dir_cloud , " and " , log_dir , "  already exist"
	gtdownload_cmdi_template_cloud  = 'sudo gtdownload -v -k 15 -c ' + '/home/ubuntu/GeneTorrent/cghub.key -R /home/ubuntu/GeneTorrent -p ' + target_dir_cloud + ' --max-children 6 "https://cghub.ucsc.edu/cghub/data/analysis/download/'

  	s3conn = boto.connect_s3( )
  	request_s3_bucket =  s3conn.get_bucket(request_bucket)
#
# 
#   Local Environment
#  
  else:
	print " local Environment"
        max_messages_to_handle = 1
        statistics_queue_name = 'local_25kStatisticsQueue'
        queue_name = 'local_25kRequestQueue'
        analysis_queue_name = 'local_25kAnalysisQueue'
        visibility_timeout = 1800
	# target_dir_local = os.getcwd() + '/download'
	target_dir_local = '/ngs/oncology/datasets/external/tcga/25k/25k_0001'
	log_dir = target_dir_local + '/log'
	try:
		create_dir(target_dir_local)
		create_dir(log_dir)
	except:
		print " Folders : " , target_dir_local , " and " , log_dir , "  already exist" 
  	keypath = ' /group/cancer_informatics/tools_resources/NGS/cghub'
	gtdownload_cmdi_template = 'gtdownload -v -k 15 -c ' + keypath +'/cghub.key -R ./GeneTorrent -p ' + target_dir_local + ' --max-children 6 "https://cghub.ucsc.edu/cghub/data/analysis/download/'
  
#
# Open a Queue inthe east region - 
#  
  conn = boto.sqs.connect_to_region("us-east-1")
  print " Queue , Analysis queue , Stats Queue : " , queue_name , "  - " , analysis_queue_name , " - " , statistics_queue_name

  q = get_queue_by_name(conn, queue_name,visibility_timeout)
  analysis_queue = get_queue_by_name(conn, analysis_queue_name,visibility_timeout)
  statistics_queue = get_queue_by_name(conn,statistics_queue_name,visibility_timeout)


#
# Handle Messages
#
  round = 1
  signal.signal(signal.SIGINT, signal_handler)
  #Ignore SIG_PIPE and don't throw exceptions on it... (http://docs.python.org/library/signal.html)
  signal.signal(SIGPIPE,SIG_DFL) 
  while True:
	print ">>>>>>>>>>> S3 Ingestion Worker , on round %d now !" % (round)
        round += 1
	# if round == 100000:      
	#	print "Exiting ................."
	#	sys.exit()

        response = q.get_messages(1)
	got_messages = len(response)
	
	if got_messages == 0 :
		print "No messages in queue"
                time.sleep(20)
	else:
  		m = response[0]
  		body = m.get_body()
  		# print body
  
		jsonResponse = json.loads(body)
		print " message: >> " , jsonResponse
		uuid = jsonResponse['analysis_id']
		file_name = jsonResponse['file_1-filename']
		file_size = jsonResponse['file_1-filesize']
                print "uuid : " , uuid , "  file_name: " , file_name
		s3_key = uuid +  '.' + file_name 
		#
		# Move to next request of key is already in the bucket
		#
		if environment == 'cloud':
			if key_exist(request_s3_bucket , s3_key):
				print ">>>>>>> Print Message Deleted ,  key already exist : ", s3_key
				m.delete()
		 		continue
		else:
			print " local environment"	
		#
		# if Request file size is bigger than available space
		# assuming we are putting our scratch space  on /mnt
		# in this caase we do not delete message as it will go back to the pool
		#
		mnt_free_space = get_fs_freespace('/mnt') 
		# print "Checking if we can handle this file size:  free:" , mnt_free_space , " File Size: " , file_size
		if long( get_fs_freespace('/mnt') ) < long (file_size ) :
			print ("Not enough space in /mnt partition , skip this request" )
			m.change_visibility(0)   # get the message back to the queue
			print "go to sleep and let other have a chance..."
                	time.sleep(20)
			continue	

                analysis_message_data = json.loads(body)

  		# for item in jsonResponse:
      		# 	print "itemi:" , item , "  Data:" , jsonResponse[item] , '\n'
		if environment == 'cloud':
  			gtcmd = gtdownload_cmdi_template_cloud + uuid + '"' 
			target_dir = target_dir_cloud
		else:
			gtcmd = gtdownload_cmdi_template + uuid  + '"'
			target_dir = target_dir_local

		gtcmd = gtcmd + ' -l ' + log_dir + '/log.' + uuid + '6568.txt:full'   ## if we want a more verbose one we can put full instead of standards
		try:
                        # download cghub files
			print gtcmd
			start_time = timeit.default_timer()
			try:
				os.system(gtcmd)
			except:
				print "Issue running gtcmd "
				pass
			print " After gtcmd exucuted"  
			elapsed = timeit.default_timer() - start_time
                        overall_rate = float(file_size) / float(elapsed) / float(1024) / float(1024)
			#print "uuid: " , uuid , " fileSize (bypes): " , file_size , " elapsed time (sec) : " , elapsed , " Overall Rate: " , overall_rate , " MB/s" 
                        # push files to s3
                        analysis_queue_data = {}
			# get  files urls
			if environment == 'cloud':
				print " cloud Environment , push file to s3"
	                        analysis_queue_data  = push_to_s3(request_bucket, uuid , get_filepaths(target_dir + '/' + uuid))
				# Clean up local system as files sent to s3
				print " Clean up local files as needed"
                               	return_code = cleanup_files(environment, target_dir + '/' + uuid)
				print "after clean up"
			else:
				print "local environment  files are left on local system files"
				analysis_queue_data  = get_local_dowloaded_files( uuid , get_filepaths(target_dir + '/' + uuid))

			print 'Files  url list:' , analysis_queue_data 
			#

			if len(analysis_queue_data) == 0:
				print " Empty files mean no objects pushed to S3 -" , '\n' , " aleardy cleanedup so just get the emssage back to pool"
				m.change_visibility(0)   # get the message back to the queue
				round = 1
				continue
			else:	
				# prepare analysis queue message and added it to the analysis queue
				#
				print " Files ready in target directory , need to wrapup "
				for item in jsonResponse:
                                	analysis_queue_data[item] = jsonResponse[item]
				# add more meta data 
				analysis_queue_data['version'] = code_version 
				analysis_queue_data['environment'] = environment
				analysis_queue_data['date'] = time.strftime("%X")
                                analysis_queue_data['time'] = time.strftime("%X")
				analysis_queue_data['system'] = os.uname()
				# print "analysis queue data : " , analysis_queue_data
				json_data = json.dumps(analysis_queue_data)
				#print json_data
        			analysis_message = Message()
        			analysis_message.set_body(json_data)
        			analysis_queue.write(analysis_message)
				print " Analysis Queue message written"

                                # log gtdownload info for statistics
				statistics_data = {}
				statistics_data['version'] = code_version 
				statistics_data['date'] = time.strftime("%X")
				statistics_data['time'] = time.strftime("%X")
				statistics_data['system'] = os.uname()
				statistics_data['target'] = 'gtdownload'
				statistics_data['uuid'] = uuid
				statistics_data['FileSize'] = file_size
				statistics_data['ElapsedTime'] = elapsed
				json_data = json.dumps(statistics_data)
                                statistics_message = Message()
				statistics_message.set_body(json_data)
                                statistics_queue.write(statistics_message)
				# Clean up	
				print " Delete message from Request pool as we uploaded the files"
				m.delete()
				print "Done with this message "
				round = 1
		except:
			print ">>>>>>>>>  Issues in cmd I am going out "
			sys.exit(0)
#
# Cleanup
#

if __name__ == "__main__":
   main(sys.argv[1:])

