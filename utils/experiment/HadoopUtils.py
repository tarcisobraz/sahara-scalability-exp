__author__ = 'tarciso'

import subprocess
import os
import time
from linuxUtils import LinuxUtils

class HadoopUtils():
    @staticmethod
    def runHDFSCommand(args):
        command = ["/usr/local/hadoop/bin/hdfs", "dfs"]
        command += args.split()

        print command

        proc = subprocess.Popen(command,stdout=subprocess.PIPE)
        proc.wait()
        return (proc.communicate(),proc.returncode)

    @staticmethod
    def checkHDFSPathExists(path):
        return HadoopUtils.runHDFSCommand("-test -e " + path)[1] == 0


    @staticmethod
    def runHadoopStreamingJob(input,output,mapperCommand,reducerCommand=None,numReducerTasks=None,filesArray=None):
        command = ["hadoop","jar","/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar"]

        if (filesArray != None):
            command += ["-files"]
            filesStr = ""
            for i in range(0,len(filesArray)):
                if i == 0:
                    filesStr += filesArray[i]
                else:
                    filesStr += "," + filesArray[i]
	        command += [filesStr]

        if (numReducerTasks != None):
            command += ["-Dmapreduce.job.reduces="+str(numReducerTasks)]

        command += ["-input",input,"-output",output,"-mapper",mapperCommand]

        if (reducerCommand != None):
            command += ["-reducer",reducerCommand]

        print command

        commandStr = " ".join(command)
        #print commandStr
        proc = subprocess.Popen(command,stdout=subprocess.PIPE)
        for line in proc.stdout:
            print line
        #proc.wait()
        #return proc.communicate()

    @staticmethod
    def runHadoopStreamingJobSerially(input,output,mapperCommand,reducerCommand=None):
        tmpMapperOutFile = output + "/" + "mapper-out"
        reducerOutFile = output + "/" + "part-00000"

        LinuxUtils.mkdir(output) #output dir should not exist prior to job execution
        LinuxUtils.rmPath(tmpMapperOutFile)

        for filename in os.listdir(input):
            inputFilePath = input + "/" + filename
            mapCommand = ["cat",inputFilePath,"| eval ",mapperCommand,">>",tmpMapperOutFile]
            LinuxUtils.runLinuxCommand(" ".join(mapCommand))

        if (reducerCommand == None):
            LinuxUtils.cpPath(tmpMapperOutFile,reducerOutFile)
        else:
            redCommand = ["cat",tmpMapperOutFile,"| eval ",reducerCommand,">",reducerOutFile]
            LinuxUtils.runLinuxCommand(" ".join(redCommand))

    @staticmethod
    def buildExecCommandStr(commandArray):
        enclosingQuotes = "\""
        execCommandStr = enclosingQuotes + " ".join(commandArray) + enclosingQuotes
        return execCommandStr

    @staticmethod
    def getHDFSFullPath(hdfsPath):
        fsDefaultName = "hdfs://localhost:54310"
        return fsDefaultName + "/" + hdfsPath

    @staticmethod
    def mergeHDFSFiles(hdfsInputDir,filesNamesPattern,hdfsOutputFilePath):
        LinuxUtils.runLinuxCommand("hdfs dfs -cat " + hdfsInputDir + "/" + filesNamesPattern + " | hdfs dfs -put -f - " + hdfsOutputFilePath)

    @staticmethod
    def getFileFromHDFS(hdfsFilePath,localDestPath):
        HadoopUtils.runHDFSCommand("-get " + hdfsFilePath + " " + localDestPath)

    @staticmethod
    def catFile(hdfsFilePath):
        return HadoopUtils.runHDFSCommand("-cat " + hdfsFilePath)

    @staticmethod
    def rmPath(hdfsPath):
        return HadoopUtils.runHDFSCommand("-rm -r " + hdfsPath)

    @staticmethod
    def removeHDFSDirIfExists(dirPath):
        if (HadoopUtils.checkHDFSPathExists(dirPath)):
            HadoopUtils.rmPath(dirPath)

    @staticmethod
    def getmerge(hdfsPathPattern,localDestPath):
        HadoopUtils.runHDFSCommand("-getmerge " + hdfsPathPattern + " " + localDestPath)


def main():
    start = time.time()
    HadoopUtils.runHadoopStreamingJobSerially(input="/media/tarciso/38E6970DE696CA90/LVC/base_recife/binary-orb-features/",output="/tmp",
          mapperCommand="/home/tarciso/hp-workspace/arbigdata2015/trunk/VocTreeMapReduce/ClustererMapper/Release/ClustererMapper " +
            "/home/tarciso/hp-workspace/arbigdata2015/trunk/MapReduceClassifier/Clusterer/recife-dataset/centroids/centroids-128-1.json 1",
            reducerCommand="/home/tarciso/hp-workspace/arbigdata2015/trunk/VocTreeMapReduce/ClustererReducer/Release/ClustererReducer")
    end = time.time()
    print "Time: ",end - start

if __name__ == "__main__":
    main()
