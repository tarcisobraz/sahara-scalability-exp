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

MIN_NUM_ARGS = 3

def configureInstances(instancesIps, publicKeyPairPath, privateKeyPairPath):
    print "Configuring Instances..."
    for instanceIp in instancesIps:
            commandArray = ["./SSHWithoutPassword.sh", instanceIp , publicKeyPairPath, privateKeyPairPath]
            command = ' '.join(commandArray)
            print command
            subprocess.call(command,shell=True)
            print "Configured instance with IP:", instanceIp

def printUsage():
    print "python create_cluster.py <cluster_name> <config_file_path>"

if (len(sys.argv) < MIN_NUM_ARGS):
    print "Wrong number of arguments"
    printUsage()
    exit(1)

cluster_name = sys.argv[1]
config_file_path = sys.argv[2]

user = raw_input('OpenStack User: ')
key = getpass.getpass(prompt='OpenStack Password: ')
json_parser = JsonParser(config_file_path)

project_name = json_parser.get('project_name')
project_id = json_parser.get('project_id')
main_ip = json_parser.get('main_ip')
image_id = json_parser.get('image_id')
net_id = json_parser.get('net_id')
template_id = json_parser.get('cluster_template_id')
key_pair = json_parser.get('keypair_name')
public_keypair_path = json_parser.get('public_keypair_path')
private_keypair_path = json_parser.get('private_keypair_path')
connector = ConnectionGetter(user, key, project_name, project_id, main_ip)

keystone_util = UtilKeystone(connector.keystone())
token_ref_id = keystone_util.getTokenRef(user, key, project_name).id
sahara_util = UtilSahara(connector.sahara(token_ref_id))

cluster_id = sahara_util.createClusterHadoop(cluster_name, image_id, template_id, net_id,key_pair)
#cluster_id = "e5b33c1a-a6af-430d-bc0d-531c982f8017"
print cluster_id
instancesIps = sahara_util.get_instances_ips(cluster_id)
configureInstances(instancesIps,public_keypair_path,private_keypair_path)
master_ip = sahara_util.get_master_ip(cluster_id)

