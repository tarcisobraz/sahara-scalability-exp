import subprocess
import os
import sys
import getpass

MIN_NUM_ARGS = 3

def printUsage():
        print "python add-job-bins.py <binsFolderPath> <containerName>"

if (len(sys.argv) < MIN_NUM_ARGS):
        print "Wrong number of arguments:", len(sys.argv)
        printUsage()
        exit(1)

binsFolderPath = sys.argv[1]
containerName = sys.argv[2]
user = raw_input("OpenStack username: ")
pwd = getpass.getpass(prompt='OpenStack Password: ')


bins = [ f for f in os.listdir(binsFolderPath) if os.path.isfile(os.path.join(binsFolderPath,f)) ]
for binary in bins:
	command = "sahara job-binary-create --name " + binary + " --url swift://libs/" + binary + " --user " + user + " --password " + pwd
	print command
	subprocess.call(command,shell=True)
print "Done!"
