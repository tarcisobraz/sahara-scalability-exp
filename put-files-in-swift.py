import subprocess
import os
import sys
import getpass

MIN_NUM_ARGS = 4

def printUsage():
        print "python put-files-in-swift.py <inputFolderPath> <filesListFilePath> <containerName>"

if (len(sys.argv) < MIN_NUM_ARGS):
        print "Wrong number of arguments:", len(sys.argv)
        printUsage()
        exit(1)

inputFolderPath = sys.argv[1]
filesListFilePath = sys.argv[2]
containerName = sys.argv[3]

os.chdir(inputFolderPath)

with open(filesListFilePath,'r') as filesList:
	for f in filesList:
		command = "swift upload " + containerName + " " + f
		print command
		subprocess.call(command,shell=True)
print "Done!"
