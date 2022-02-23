import parmFileParser as parser
from sys import argv
import subprocess
import os 
import s3Push

class sourceFileProcessing:
    def __init__(self, sourceFileName='', jobName='', parmFileName='', parmDetails=None, pSourceFilePath='',bucketName=''):
        self.fileExtn = ''
        self.sourceFileName = sourceFileName 
        self.jobName = jobName
        self.parmFileName = parmFileName
        self.parmDetails = None
        self.sourceFilePath = pSourceFilePath
        self.sourceQcFileName = ''
        self.bucketName=bucketName
        

    def getJobDetails(self):
        print("calling the parm file parser module")
        self.parmDetails = parser.getParmDetails(self.jobName, self.parmFileName)
        self.fileExtn = self.parmDetails.get('pextn')
        self.sourceFileName = self.parmDetails.get('psourcefilename')
        self.sourceFilePath = self.parmDetails.get('psourcefilepath')
        self.sourceQcFileName=self.sourceFileName+'.qc'
        self.bucketName=self.parmDetails.get('ps3bucketname')

        if self.sourceFileName is None or self.fileExtn is None or self.sourceFilePath is None:
            print("parameters missing in the parm file exiting the script")
            exit(1)

        print(self.parmDetails)

    def qcValidation(self):
    
        ''' we can use the below line to get the main source file count from unix commands by importing from subprocess import check_output
        srcLineCount=int(check_output(["wc", "-l", self.sourceFullFileName]).split()[0])'''
        print("validating the qc file count against the file line count")
        print("opening the qc file", self.sourceFileName + '.qc')
        qcFileCount = 0
        qcFileFullPath = self.sourceFilePath + '/' + self.sourceQcFileName
        #print("qcFileFullPath=",qcFileFullPath)
        with open(qcFileFullPath, 'r') as f:
            qcFileCount=int(f.read().split('~')[1])
        print("qc file count is ",qcFileCount)
        
        sourceFileCount=0
        sourceFileFullPath=self.sourceFilePath +'/' +self.sourceFileName + self.fileExtn
        print("getting the main source file count for qc validation ",sourceFileFullPath)
        with open(sourceFileFullPath,'r') as f:
            sourceFileCount=len(f.readlines())
            
        if sourceFileCount != qcFileCount:
            print(f"qc validation failed main file count {sourceFileCount} and qc file count {qcFileCount}. Exiting the script")
            exit(2)
        print("source file count",sourceFileCount)


    def fileArchival():
        pass
        
    def pushFileDropzone():
        ''' After checking the qc validation pushing the file to dropzone for further processing and converting the file to parquet '''
        

print("command line arguments ", argv)

jobName = argv[1]
parmFileName = argv[2]

print(jobName, parmFileName)

fp = sourceFileProcessing(jobName=jobName, parmFileName=parmFileName)
fp.getJobDetails()
fp.qcValidation()

print("calling class awsfilePush")
s3push=s3Push.awsfilePush(bucketName=fp.bucketName,bucketKey='/'+fp.sourceFileName,filePath=fp.sourceFilePath+'/'+fp.sourceFileName+fp.fileExtn)
s3push.singlePartUpload()