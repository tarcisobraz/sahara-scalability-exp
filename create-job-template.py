import os
import sys
import subprocess
import getpass

MIN_NUM_ARGS = 3
DEF_JOB_TYPE = "MapReduce.Streaming"

def printUsage():
        print "python create-job-template.py <jobTemplateName> <binsFolderPath> "

def getBinId(binName):
	proc = subprocess.Popen("sahara job-binary-show --name " + binName + " | grep ' id'",shell=True,stdout=subprocess.PIPE)
	proc.wait()
	(out,err) = proc.communicate()
	binId = out.split("|")[2].strip()
	print "Retrieved BinId:",binId
	return binId
	

if (len(sys.argv) < MIN_NUM_ARGS):
        print "Wrong number of arguments:", len(sys.argv)
        printUsage()
        exit(1)

jobTemplateName = sys.argv[1]
binsFolderPath = sys.argv[2]
user = raw_input("OpenStack username: ")
pwd = getpass.getpass(prompt='OpenStack Password: ')


bins = [ f for f in os.listdir(binsFolderPath) if os.path.isfile(os.path.join(binsFolderPath,f)) ]
binsIds = []
for binary in bins:
	binsIds.append(getBinId(binary))

binsStr = ""
for binId in binsIds:
	binsStr += " --lib " + binId
print binsStr 

subprocess.call("sahara job-template-create --name " + jobTemplateName + " --type " + DEF_JOB_TYPE + " " + binsStr,shell=True)
