__author__ = 'tarciso'

import subprocess

class LinuxUtils():
    @staticmethod
    def runLinuxCommand(command):
        print command
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
        return (proc.communicate(),proc.returncode)

    @staticmethod
    def checkPathExists(path):
        command = "test -e " + path
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
        proc.wait()
        return proc.returncode == 0

    @staticmethod
    def mkdir(dirPath):
        command = "mkdir -p " + dirPath
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
        proc.wait()
        return proc.returncode == 0

    @staticmethod
    def rmPath(path):
        command = "rm -rf " + path
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
        proc.wait()
        return proc.returncode == 0

    @staticmethod
    def cpPath(srcPath,dstPath):
        command = "cp -r " + srcPath + " " + dstPath
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
        proc.wait()
        return proc.returncode == 0

    @staticmethod
    def catFile(filePath):
        command = "cat " + filePath
        return LinuxUtils.runLinuxCommand(command)

def main():
    print LinuxUtils.catFile("/home/tarciso/Downloads/part-copy")[0][0]

if __name__ == "__main__":
    main()
