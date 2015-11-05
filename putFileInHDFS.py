import subprocess
import os
import time
import sys

MIN_NUM_ARGS = 3
DEF_HADOOP_USER_PASSWD = "hadoop123"
MOUNT_FOLDER_PATH = "/mnt/database"
DEF_DEVICE_PATH = "/dev/vdc"

def printUsage():
    print "python PutFileInHDFS.py <filePathInVolume> <dirName>"

def runHDFSCommand(args):
    command = ["/opt/hadoop/bin/hdfs", "dfs"]
    command += args.split()

    print command
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    proc.wait()
    return (proc.communicate(),proc.returncode)

def createHDFSDir(dirPath):
    runHDFSCommand("-mkdir -p " + dirPath)

def putFileInHDFS(filePath, destPath="", blockSize=None):
    if blockSize != None:
        runHDFSCommand("-Ddfs.block.size=" + blockSize + " -put " + filePath + " " + destPath)
    else:
        runHDFSCommand("-put " + filePath + " " + destPath)

def putFilesInHDFS(folderPath, destPath="", blockSize=None):
    files = [ os.path.join(folderPath,f) for f in os.listdir(folderPath) if os.path.isfile(os.path.join(folderPath,f)) ]
    for f in files:
        if blockSize != None:
            runHDFSCommand("-Ddfs.block.size=" + blockSize + " -put " + f + " " + destPath)
        else:
            runHDFSCommand("-put " + f + " " + destPath)

def mountVolume():
    command = 'echo ' + DEF_HADOOP_USER_PASSWD + ' | sudo -S mkdir -p' + ' ' + MOUNT_FOLDER_PATH
    print command
    subprocess.call(command, shell=True)
    command = 'echo ' + DEF_HADOOP_USER_PASSWD + ' | sudo -S mount' + ' ' + DEF_DEVICE_PATH + ' ' + MOUNT_FOLDER_PATH
    print command
    subprocess.call(command, shell=True)

def umountVolume():
    command = 'echo ' + DEF_HADOOP_USER_PASSWD + ' | sudo -S umount ' + DEF_DEVICE_PATH
    print command
    subprocess.call(command, shell=True)

if (len(sys.argv) < MIN_NUM_ARGS):
    print "Wrong number of arguments: ", len(sys.argv)
    printUsage()
    exit(1)

file_path = sys.argv[1]
dir_name = sys.argv[2]

file_path = MOUNT_FOLDER_PATH + "/" + file_path
print file_path
mountVolume()
createHDFSDir(dir_name)
#putFileInHDFS(file_path, dir_name)
putFilesInHDFS(file_path, dir_name)
#putFilesInHDFS("/home/hadoop/test_input",dir_name)
umountVolume()
