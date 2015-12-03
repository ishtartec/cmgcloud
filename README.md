# cmgcloud
Manage start and stop of Cloudera Manager clusters in Google Cloud through CM and GC API

	Usage: cmgcloud.py [options]

	Options:
  	-h, --help            show this help message and exit
  	-H CM_HOST, --host=CM_HOST
                        The Cloudera Manager host i.e: 93.44.55.66
  	-U USERNAME, --user=USERNAME
                        Admin user name
  	-P PASSWORD, --password=PASSWORD
                        Password of the admin user name
  	-O OPERATION, --operation=OPERATION
                        Operation to perform on the cluster: start or stop
  	-G, --google-cloud    Start/Stop Google Cloud instances aswell
  	-J GCPROJECT, --project=GCPROJECT
                        Specify Google Cloud Project
  	-Z GCZONE, --zone=GCZONE
                        Specify Google Cloud Zone
  	-v, --verbose         Enable Verbose Output.  
  	
## Examples

Before doing any Google Cloud operation you need to login:

	gcloud auth login
	
Be sure to give the right permissions to the script:

	chmod +x gmcloud.py

Start the Google Cloud instances and Hadoop Cluster Services

	./cmgcloud.py -H cm.example.com -U admin -P admin -O start -J google_cloud_project1234 -Z europe-west1-b -G	
Stop the Hadoop Cluster Services and Google Cloud instances

	./cmgcloud.py -H cm.example.com -U admin -P admin -O stop -J google_cloud_project1234 -Z europe-west1-b -G
	
Start the Hadoop Cluster services only

	./cmgcloud.py -H cm.example.com -U admin -P admin -O start

Stop the Hadoop Cluster services only
	
	./cmgcloud.py -H cm.example.com -U admin -P admin -O stop
	
## Dependencies

Google Cloud SDK: [https://cloud.google.com/sdk/](https://cloud.google.com/sdk/)

	sudo pip install --upgrade google-api-python-client

Cloudera Manager API (python bindings):

	sudo pip install cm-api
