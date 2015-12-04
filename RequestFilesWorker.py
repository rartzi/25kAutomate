#!/usr/local/bin/python
import sys, getopt
import boto.sqs
import csv
import json 

from boto.sqs.message import Message

def main(argv):
#  print 'Number of arguments:', len(sys.argv), 'arguments.'
#  print 'Argument List:', str(sys.argv)
  
  file_name = 'list.csv'
  queue_name = '25kRequestQueue'
  aws_region = 'us-east-1'
# Queue Visibility timeout will be 1 hour before anotehr wrker can handle the request, 
  visibility_timeout = 3600
  environment = 'local'
#
#    Handle Input paramenters : FeedInRequest --qname=Queue --fname=filename with uuids
#
  try:
     opts, args = getopt.getopt(argv,"hq:f",["qname=","fname="])
  except getopt.GetoptError:
        # print 'FeedRequestIn.py -q <queu_name> -f <fname>'
        sys.exit(2)
  
  for opt, arg in opts:
      print  opt
      if opt == '-h':
         print 'FeedRequestIn.py -q <queue_name> -f <request_file_name>'
         sys.exit()
      elif opt in ("-q", "--qname"):
         queue_name = arg
      elif opt in ("-f", "--fname"):
	 file_name = arg
  
  print "Queue Name ", queue_name 
  print "Reuest File Name ", file_name

#
#   Cloud
#
#
  if ( environment == 'cloud' ):
        print "Cloud Environment "
        queue_name = '25kRequestQueue'
        visibility_timeout = 1800
#
#
#   Local Environment
#
  else:
        print " local Environment"
        queue_name = 'local_25kRequestQueue'
        visibility_timeout = 1800


#
# Open a Queue inthe east region - needs to remove the access keys from script
#  
  conn = boto.sqs.connect_to_region(aws_region)
  print " Creating Request Queue: " , queue_name
  q = conn.create_queue(queue_name,visibility_timeout)

#
#   Parsing input file and for each line create a queue message to be handled by workers.
#
  f = open(file_name)
  csv_f = csv.DictReader(f, delimiter='\t' , skipinitialspace=True)
  csv_headers = csv_f.fieldnames
  print "csv_headers:" , csv_headers , "len: " , len(csv_headers)
  for row in csv_f:
      	data = {}
      	for field in csv_headers:
		# print " field: " , field , "   row: " , row[field]
          	data[field] = row[field]
      	json_data = json.dumps(data)
      	# print json_data
 	m = Message()
  	m.set_body(json_data)
  	q.write(m)

if __name__ == "__main__":
   main(sys.argv[1:])

