#! /usr/bin/env python
"""Manages a Cloudera Hadoop Cluster start and stop process.
"""
# sudo pip install cm-api
# sudo pip install --upgrade google-api-python-client

import logging
import sys
from optparse import OptionParser
# Get a handle to the API client
from cm_api.api_client import ApiResource

# Get a handle to the oAuth2 Api
from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

from time import sleep

__author__ = "Sergio Rodriguez"
__license__ = "GPL"
__version__ = "0.1.0"
__maintainer__ = "Sergio Rodriguez"
__email__ = "sergio@ishtar.io"
__status__ = "Development"


parser = OptionParser()
parser.add_option('-H', '--host', type='string', dest='cm_host', help='The Cloudera Manager host i.e: 93.44.55.66')
parser.add_option('-U', '--user', type='string', dest='username', default='admin', help='Admin user name')
parser.add_option('-P', '--password', type='string', dest='password', default='admin', help='Password of the admin user name')
parser.add_option('-O', '--operation', type='string', dest='operation', help='Operation to perform on the cluster: start or stop')
parser.add_option('-G', '--google-cloud', dest='switch_gc_instances', default=False, help='Start/Stop Google Cloud instances aswell', action='store_true')
parser.add_option('-J', '--project', type='string', dest='gcproject', help='Specify Google Cloud Project')
parser.add_option('-Z', '--zone', type='string', dest='gczone', help='Specify Google Cloud Zone')
parser.add_option('-v', '--verbose', dest='verbose', default=False, help='Enable Verbose Output.', action='store_true')
(options, args) = parser.parse_args()

if options.cm_host is None:
    logging.error('Please specify a CM host with -H or --host.')
    parser.print_help()
    sys.exit(-1)
if options.operation is None:
    logging.error('Please specify an operation with -O start/stop or --operation start/stop.')
    parser.print_help()
    sys.exit(-1)
if options.switch_gc_instances is None or options.gcproject is None:
    logging.error('Please specify Google Cloud Project with -J or --project.')
    parser.print_help()
    sys.exit(-1)
if options.switch_gc_instances is None or options.gczone is None:
    logging.error('Please specify Google Cloud Zone with -Z or --zone.')
    parser.print_help()
    sys.exit(-1)
if options.verbose:
    logging.basicConfig(
        level=logging.INFO,
    )

cm_host = options.cm_host
api = ApiResource(cm_host, username=options.username, password=options.password)

def shutdown_cluster():
	# Get a list of all clusters
	cdh5 = None
	for c in api.get_all_clusters():
		logging.info('Found cluster: ' + c.name)
		if c.version == "CDH5":
			cdh5 = c
			for s in cdh5.get_all_services():
				logging.info(s)
			logging.info("Now stopping " + c.name)
			cmd = c.stop()
			print "Stopping the cluster. This might take a while."
			while cmd.success == None and len(c.get_commands()) > 0:
				sleep(5)
				logging.info("Pending jobs: " + str(len(c.get_commands())))
			
			cmd = cmd.fetch()

			if cmd.success != True:
				print "Cluster  " + c.name + " stop failed: " + cmd.resultMessage
				exit(0)

    		print "Cluster " + c.name + " stop succeeded"
				#if s.type == "HDFS":
				#	hdfs = s
				#	print logging.info(hdfs.name + ":" + hdfs.serviceState + ":" + hdfs.healthSummary)
  
def start_cluster():
	# Get a list of all clusters
	cdh5 = None
	for c in api.get_all_clusters():
		logging.info('Found cluster: ' + c.name)
		if c.version == "CDH5":
			cdh5 = c
			for s in cdh5.get_all_services():
				logging.info(s)
			logging.info("Now starting " + c.name)
			cmd = c.start()
			print "Starting the cluster. This might take a while."
			while cmd.success == None and len(c.get_commands()) > 0:
				sleep(5)
				logging.info("Pending jobs: " + str(len(c.get_commands())))
			
			cmd = cmd.fetch()

			if cmd.success != True:
				print "Cluster  " + c.name + " start failed: " + cmd.resultMessage
				exit(0)

    		print "Cluster " + c.name + " start succeeded"

def get_google_credentials():
	credentials = GoogleCredentials.get_application_default()
	compute = discovery.build('compute', 'v1', credentials=credentials)

# [START list_instances]
def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items']
# [END list_instances]

# [START start_instance]
def start_instance(compute, project, zone, name):
    return compute.instances().start(
        project=project,
        zone=zone,
        instance=name).execute()
# [END start_instance]

# [START stop_instance]
def stop_instance(compute, project, zone, name):
    return compute.instances().stop(
        project=project,
        zone=zone,
        instance=name).execute()
# [END stop_instance]

# [START wait_for_operation]
def wait_for_operation(compute, project, zone, operation):
	print('Waiting for operation to finish...')
	while True:
	        result = compute.zoneOperations().get(
				project=project,
				zone=zone,
				operation=operation).execute()

	        if result['status'] == 'DONE':
				print("done.")
				if 'error' in result:
					raise Exception(result['error'])
				return result

	        sleep(1)
# [END wait_for_operation]

# [START start_gc_cluster]
def start_gc_cluster():
	credentials = GoogleCredentials.get_application_default()
	compute = discovery.build('compute', 'v1', credentials=credentials)
	instances = list_instances(compute, options.gcproject, options.gczone)
	print('Instances in project %s and zone %s:' % (options.gcproject, options.gczone))
	for instance in instances:
		print(' - ' + instance['name'] + ': Starting instance...')
		operation = start_instance(compute, options.gcproject, options.gczone, instance['name'])
    	wait_for_operation(compute, options.gcproject, options.gczone, operation['name'])
# [END start_gc_cluster]

# [START stop_gc_cluster]
def stop_gc_cluster():
	credentials = GoogleCredentials.get_application_default()
	compute = discovery.build('compute', 'v1', credentials=credentials)
	instances = list_instances(compute, options.gcproject, options.gczone)
	print('Instances in project %s and zone %s:' % (options.gcproject, options.gczone))
	for instance in instances:
		print(' - ' + instance['name'] + ': Stopping instance...')
		operation = stop_instance(compute, options.gcproject, options.gczone, instance['name'])
    	wait_for_operation(compute, options.gcproject, options.gczone, operation['name'])
# [END stop_gc_cluster]

def main():
	
	#wait_for_operation(compute, project, zone, operation['name'])

	if options.operation == "start":
		if options.switch_gc_instances:
			start_gc_cluster()
			# Should wait something for CM to start as a service or check open port
			sleep(60)
		start_cluster()

	if options.operation == "stop":
		shutdown_cluster()
		if options.switch_gc_instances:
			stop_gc_cluster()

	


if __name__ == "__main__":
    main()

