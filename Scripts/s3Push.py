'''creating a class to define methods to push the file to s3 for the specific folders using boto3
   its a module which will be called in any of the packages by passing the required paramter
'''
import boto3
from boto3.s3.transfer import TransferConfig
import os



class awsfilePush:
    def __init__(self,bucketName=None,bucketKey=None,filePath=None):
        self.bucketName=bucketName
        self.bucketKey=bucketKey
        self.s3ClientResource = boto3.resource('s3')
        self.s3Bucket=boto3.client('s3')
        self.filePath=filePath
        
        
    def singlePartUpload(self):
        ''' Define the Transfer config properties to decide based on the file size it will trigger the multi part upload
        and max_concurrency is the number of threads running for below example we making 2500 mb then we are triggering multipart
        boto3 syntax 
        upload_file(Filename, Bucket, Key, ExtraArgs=None, Callback=None, Config=None)
        import boto3
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file('/tmp/hello.txt', 'mybucket', 'hello.txt')
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
        '''
        
        config = TransferConfig(multipart_threshold=1024 * 2500,
                        max_concurrency=10,
                        multipart_chunksize=1024 * 2500,
                        use_threads=False)
        print("Calling the boto3 aws sdk upload_file")
        res=self.s3ClientResource.Object(self.bucketName, self.bucketKey).upload_file(self.filePath,
                            ExtraArgs=None,
                            Config=config
                            )
        
        print("inside s3push",res)
        
    
    
    def multiPartUpload():
        pass