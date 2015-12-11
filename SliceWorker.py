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
  code_version = '1.0' # remember to change on message stracture changes
  #
  statistics_queue_name = '25kStatisticsQueue'
  source_queue ='25kAutomationResultsQueue'
  target_queue_name = '25kResultReadyForAZUpload'
  target_bucket = 'az-ngs-25k-sliced-bam-results'
#  target_bucket = 'az-ngs-test25k'
  target_bucket_name = 's3://' + target_bucket
  visibility_timeout = 1800
  environment = 'cloud'
  target_fs = '/'
  max_failures_allowed = 4   # max times we will retry the same messages nd increase the delay will we are moving on.
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
         print 'myname.py -q <source_queue> -e <max_messages>'
         sys.exit()
      elif opt in ("-q", "--qname"):
         source_queue = arg
      elif opt in ("-e", "--env"):
          environment = arg

  print "Queue Name ", source_queue
  print "environment: ", environment

#
#   Cloud 
#
#
  if ( environment == 'cloud' ):
	print "Cloud Environment "
	statistics_queue_name = '25kStatisticsQueue'
	source_queue =  source_queue
	target_queue_name = target_queue_name
	target_bucket = target_bucket
	#  target_bucket = 'az-ngs-test25k'
	target_bucket_name = 's3://' + target_bucket
  	visibility_timeout = 1800
	target_fs = '/mnt'          
	# target_fs = '/'
	try:
		target_dir_cloud = target_fs + '/download'
		log_dir = target_dir_cloud + '/log'
		create_dir(target_dir_cloud)
        	create_dir(log_dir)
	except:
		print " Folders : " , target_dir_cloud , " and " , log_dir , "  already exist"

  	s3conn = boto.connect_s3( )
  	request_s3_bucket =  s3conn.get_bucket(target_bucket)
#
# 
#   Local Environment
# 
  else:
	print " local Environment"
        statistics_queue_name = 'local_25kStatisticsQueue'
        source_queue = 'local_25kRequestQueue'
        target_queue_name = 'local_25kAnalysisQueue'
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
  print " Queue , Analysis queue , Stats Queue : " , source_queue , "  - " , target_queue_name , " - " , statistics_queue_name

  q = get_queue_by_name(conn, source_queue,visibility_timeout)
  target_queue = get_queue_by_name(conn, target_queue_name,visibility_timeout)
  statistics_queue = get_queue_by_name(conn,statistics_queue_name,visibility_timeout)


#
# Handle Messages
#
# Initiatl State
  round = 0
  failure_round  = 0
  respose = ' ' 
  got_messages = 0
  signal.signal(signal.SIGINT, signal_handler)
  #Ignore SIG_PIPE and don't throw exceptions on it... (http://docs.python.org/library/signal.html)
  signal.signal(SIGPIPE,SIG_DFL) 
  while True:
	print ">>>>>>>>>>> BAM Slice  Worker , on round %d now !" % (round)
        round += 1
	if round == 2 :      
		print "Exiting ................."
		sys.exit()
	if failure_round == 0 :
        	response = q.get_messages(1, visibility_timeout = 60 ) ############# Ronen Fix visibility when we are working production
		got_messages = len(response)
	else:
		delay = failure_round * 60
		print " >>>>>>>>>>>>    This will be a repeat round after failure attempt : " , failure_round , " Going to sleep for 10 + " , delay , " Seconds"  
		time.sleep( 10 + delay )
		# failure_round +=1   # This will be incremented at point of failures

	if got_messages == 0 :
		print "No messages in queue"
                time.sleep(20)
	else:
  		m = response[0]
  		body = m.get_body()
  		# print body
  
		jsonResponse = json.loads(body)
		# print " message: >> " , jsonResponse
		uuid = jsonResponse['analysis_id']
		legacy_sample_id = jsonResponse['legacy_sample_id']
		automation_step_request = jsonResponse['AutomationStepRequest']
		realigned_bam_file_url = jsonResponse['RealignedBamFileURL']
		file_name = jsonResponse['file_1-filename']
		file_size = jsonResponse['file_1-filesize']
                print "uuid : " , uuid , "  legacy sample id : " , legacy_sample_id , " AutomationStepRequest: " , automation_step_request
		s3_key = uuid +  '.' + legacy_sample_id + '.bam'
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
		mnt_free_space = get_fs_freespace(target_fs) 
		# print "Checking if we can handle this file size:  free:" , mnt_free_space , " File Size: " , file_size
		if long( get_fs_freespace(target_fs) ) < long (file_size ) :
			print ("Not enough space in /mnt partition , skip this request" )
			m.change_visibility(0)   # get the message back to the queue
			print "go to sleep and let other have a chance..."
                	time.sleep(20)
			continue	

                analysis_message_data = json.loads(body)

  		# for item in jsonResponse:
      		# 	print "itemi:" , item , "  Data:" , jsonResponse[item] , '\n'
		if environment == 'cloud':
			target_dir = target_dir_cloud
		else:
			target_dir = target_dir_local

		samtool_slice_cmd = 'samtools view -h -L genes.bed -b -o ' + target_dir + '/' + uuid + '/'+ uuid + '.' + legacy_sample_id + '.bam ' +   realigned_bam_file_url
		samtool_stats_cmd = 'samtools idxstats ' + realigned_bam_file_url + ' > ' + target_dir + '/' + uuid + '/' + uuid + '.' + legacy_sample_id + '_stats.txt'
		rm_alignment_file_cmd = 'rm alignments.bam.bai'
		try:
                        # Slice Bams
			print '\n' , 'SamTool Slide Command : ' , samtool_slice_cmd
			print '\n' , 'SamTool Stats Command : ' , samtool_stats_cmd
			
			# Ronen
			continue


			start_time = timeit.default_timer()
			try:
				print "Running SamTool Slice "
				os.system(samtool_slice_cmd)
				print "Running SamTool Stats"
				os.system(samtool_stats_cmd)
				print "Remove alignment file"
                                os.system(rm_alignment_file_cmd)
			except:
				print "Issue running samtool commands "
				failure_round += 1 # increase failure repeat attemtps
				pass
			print " After SamTools commands exucuted"  
			elapsed = timeit.default_timer() - start_time
                        # push files to s3
                        upload_queue_data = {}
			# get  files urls
			if environment == 'cloud':
				print " cloud Environment , push file to s3"
	                        upload_queue_data  = push_to_s3(target_bucket, uuid , get_filepaths(target_dir + '/' + uuid))
				# Clean up local system as files sent to s3 
				print " Clean up local files as needed"
                               	return_code = cleanup_files(environment, target_dir + '/' + uuid)
				print "after clean up"
			else:
				print "local environment  files are left on local system files"
				upload_queue_data  = get_local_dowloaded_files( uuid , get_filepaths(target_dir + '/' + uuid))

			print 'Files  url list:' , upload_queue_data 
			#

			if len(upload_queue_data) == 0:
				print " Empty files mean no files or partial update"
				failure_round += 1 # increase failure repeat attemtps
				#
				# if we had reached the max failures on a message get it back to the pool and continue
				# other wise continue with the same message and bigger delay trying to recover
				if failure_round >= max_failures_allowed: 
					# 
					failure_round = 0        # reset failure flag so we will go to read the queue again
					m.change_visibility(0)   # get the current message back to the queue
					print " Clean up local files as needed"
                                	return_code = cleanup_files(environment, target_dir + '/' + uuid)
                                	print "after clean up"
					round = 1
				continue
			else:	
				# prepare analysis queue message and added it to the analysis queue
				#
				print " Files ready in target directory , need to wrapup "
				for item in jsonResponse:
                                	upload_queue_data[item] = jsonResponse[item]
				# add more meta data 
				upload_queue_data['version'] = code_version 
				upload_queue_data['environment'] = environment
				upload_queue_data['date'] = time.strftime("%X")
                                upload_queue_data['time'] = time.strftime("%X")
				upload_queue_data['system'] = os.uname()
				# print "analysis queue data : " , upload_queue_data
				json_data = json.dumps(files_url_list)
				#print json_data
        			upload_message = Message()
        			upload_message.set_body(json_data)
        			upload_queue.write(analysis_message)
				print " Upload Queue message written"

				# Clean up
				failure_round = 0 # we did well , no failure so we can reset.
				# We can cleanup the file system
				if environment == 'cloud':
					# Clean up local system as files sent to s3 
	                                print " Clean up local files as needed"
        	                        return_code = cleanup_files(environment, target_dir + '/' + uuid)
					print  " after clean up"	
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

