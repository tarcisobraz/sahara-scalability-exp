from utils.openstack.UtilSwift import *
from utils.openstack.ConnectionGetter import *
from utils.openstack.UtilSahara import *
from utils.openstack.UtilKeystone import *
from utils.experiment.JsonParser import *
import os
import sys
import subprocess
import time
import getpass
from subprocess import Popen, call, PIPE

MIN_NUM_ARGS = 9
DEF_CLUSTER_NAME = "experiment-cluster"
NUMBER_OF_BACKUP_EXECUTIONS = 0
HDFS_BASE_DIR = "/user/hadoop/"
HOME_INSTANCE_DIR = "/home/hadoop"
DEF_OUTPUT_CONTAINER_NAME = "output"
DEF_INPUT_DIR = "input"

def configureInstances(instancesIps, publicKeyPairPath, privateKeyPairPath):
	print "Configuring Instances..."
	for instanceIp in instancesIps:
        	commandArray = ["./SSHWithoutPassword.sh", instanceIp , publicKeyPairPath, privateKeyPairPath] 
        	command = ' '.join(commandArray)
        	print command
        	call(command,shell=True)
        	print "Configured instance with IP:", instanceIp

def copyFileToInstances(filePath,instancesIps,keypairPath):
    print "Copying " + filePath +" file to instances"
    for instanceIp in instancesIps:
        commandArray = ["scp","-i",keypairPath,"-r",filePath,"hadoop@"+instanceIp+ ':'+HOME_INSTANCE_DIR]
        command = ' '.join(commandArray)
        print command
        call(command,shell=True)
        print "Copied file to instance with IP:", instanceIp

def putFileInHDFS(filePath, masterIp,keypairPath):
    print "Copying file to cluster"
    copyFileToInstances(filePath, [masterIp],keypairPath)
    file_name = filePath.split('/')[-1]
    print file_name
    remoteFilePath = HOME_INSTANCE_DIR + '/' +  file_name
    commandArray = ["ssh -i",keypairPath,"hadoop@" + masterIp, "'cat | python -", remoteFilePath, DEF_INPUT_DIR + "'", "<", "./putFileInHDFS.py"]
    command = ' '.join(commandArray)
    print command
    call(command,shell=True)
    print "Success! File is now at HDFS of cluster!"

def createOutputSwiftDataSource(container_out_name,user,password):
	exec_date = datetime.now().strftime('%Y%m%d_%H%M%S')
	output_ds_name ="output_%s_exp_%s" % (user, exec_date)
	container_out_url = "swift://%s.sahara/%s" % (container_out_name, output_ds_name)
	data_source_out = sahara_util.createDataSource(output_ds_name,
													container_out_url,
													"swift",
													container_out_name,
													user,
													password)

	return data_source_out.id

def createHDFSDataSource(name,path):
	print "Creating Data Source in HDFS with name = " + name + " and path = " + path
	return sahara_util.createDataSource(name,
				path,
				"hdfs").id


def saveJobResult(job_res,cluster_size,output_file):
	print job_res
	result = ";".join((str(cluster_size), str(job_res['time']), job_res['status']))
	print result
	print "Finished"
	f = open(output_file, 'ab')
	f.write(result)
	f.close()


def printUsage():
	print "python runJob.py <numberExecs> <jobTemplateId> <inputDataSourceId> <mapperExecCmd> <reducerExecCmd> <numReduceTasks> <configFilePath> <outputFile>"

if (len(sys.argv) < MIN_NUM_ARGS):
	print "Wrong number of arguments: ", len(sys.argv)
	printUsage()
	exit(1)

#------------ CONFIGURATIONS -----------------
number_execs = int(sys.argv[1])
job_template_id = sys.argv[2]
input_ds_id = sys.argv[3]
mapper_exec_cmd = sys.argv[4]
reducer_exec_cmd = sys.argv[5]
mapred_reduce_tasks = sys.argv[6]
config_file_path = sys.argv[7]
output_file = sys.argv[8]

user = raw_input('OpenStack User: ')
password = getpass.getpass(prompt='OpenStack Password: ')

json_parser = JsonParser(config_file_path)

main_ip = json_parser.get('main_ip')

project_name = json_parser.get('project_name')
project_id = json_parser.get('project_id')

output_container_name = json_parser.get('output_container_name')

exec_local_path = json_parser.get('exec_local_path')
public_keypair_path = json_parser.get('public_keypair_path')
private_keypair_path = json_parser.get('private_keypair_path')
private_keypair_name = json_parser.get('private_keypair_name')
input_file_path = json_parser.get('input_file_path')

net_id = json_parser.get('net_id')
image_id = json_parser.get('image_id')

#------------ GETTING CONNECTION WITH OPENSTACK -----------------
connector = ConnectionGetter(user, password, project_name, project_id, main_ip)

keystone_util = UtilKeystone(connector.keystone())
token_ref_id = keystone_util.getTokenRef(user, password, project_name).id
sahara_util = UtilSahara(connector.sahara(token_ref_id))

#----------------------- CREATING INPUT DATASOURCE ------------------------------
#exec_date = datetime.now().strftime('%Y%m%d_%H%M%S')
#input_hdfs_name ="input_%s_exp_%s" % (user, exec_date)
#input_ds_id = createHDFSDataSource(DEF_INPUT_DIR,HDFS_BASE_DIR + "/" + DEF_INPUT_DIR) 

#----------------------- EXECUTING EXPERIMENT ------------------------------

for cluster_template in json_parser.get('cluster_templates'):
	
	cluster_template_id = cluster_template['id']
	cluster_size = cluster_template['n_slaves']
	cluster_name = DEF_CLUSTER_NAME + '-' +  str(cluster_size)

	######### CREATING CLUSTER #############
	try:
		cluster_id = sahara_util.createClusterHadoop(cluster_name, image_id, cluster_template_id, net_id, private_keypair_name)
		#cluster_id = "b9fb4e19-9540-478a-b978-d9211953ff72"
	except RuntimeError as err:
		print err.args
		break
		
	######### CONFIGURING CLUSTER ##########
	instancesIps = sahara_util.get_instances_ips(cluster_id)
	configureInstances(instancesIps, public_keypair_path, private_keypair_path)
	copyFileToInstances(exec_local_path, instancesIps, private_keypair_path)
	master_ip = sahara_util.get_master_ip(cluster_id)
	putFileInHDFS(input_file_path, master_ip,private_keypair_path)

	######### CREATING DATASOURCES ##########
	exec_date = datetime.now().strftime('%Y%m%d_%H%M%S')
	output_hdfs_name ="output_%s_exp_%s" % (user, exec_date) 
	output_ds_id = createHDFSDataSource(output_hdfs_name,HDFS_BASE_DIR + "/" + output_hdfs_name)

	######### RUNNING JOB ##########
	numFailedJobs = 0
	for i in xrange(number_execs):
		job_res = sahara_util.runStreamingJob(job_template_id, cluster_id, mapper_exec_cmd, reducer_exec_cmd, input_ds_id=input_ds_id, output_ds_id=output_ds_id)
		saveJobResult(job_res,cluster_size,output_file)
		if (job_res['status'] != 'SUCCEEDED'):
			numFailedJobs += 1
		
	
	######### DELETING CLUSTER ###########
	if (numFailedJobs <= NUMBER_OF_BACKUP_EXECUTIONS):
		pass
		#sahara_util.deleteCluster(cluster_id)
	
	print 'FINISHED FOR CLUSTER ' + cluster_name
#	sentMail()
