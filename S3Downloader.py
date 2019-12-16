'''
Title : Script to download and archive files from folder in s3 bucket.

Prerequisite : 
1. Must have AWSCLI instaled in the system.
2. Access filepaths should be prior configured by using "aws configure" in CLI

Execution : 
1. Checks whether the mentioned file is present in S3.
2. If present then Download and add "_active" to the downloaded file.
3. After download check if yyyy-mm-dd folder is present with Archive directory
4. If not present create the folder then
5. Move the file from active folder to archive/yyyy-mm-dd with prefix of name "done_"

'''
import boto
import boto3
import botocore
import os
from datetime import datetime

'''
BucketName = "ayanstestbucket"
foldername = "active"  # provide the foldername of the file without / (sla)
filename = "testfile.xlsx"  # provide the file name within the s3 folder
outPutName = "/Users/ayan.putatunda/Desktop/Aws/testfile_active.xlsx"

TODO : make the config read from file
'''

def download_file_from_s3(BucketName,foldername,filename,outPutName):
    #set filepath
    filepath = foldername + "/" + filename

    # download file and add "_active" to the downloaded file 
    s3 = boto3.resource('s3')
    try:
        s3.Bucket(BucketName).download_file(filepath, outPutName)
        print('The file has been downloaded in :'+str(outPutName))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist in S3 bucket")
        else:
            raise

    cur_date = datetime.today().strftime('%Y-%m-%d')
    check_folder = "archive/"+cur_date+"/"    # full file path
    bucket = s3.Bucket(BucketName)
    obj = list(bucket.objects.filter(Prefix=check_folder))
    s3_client = boto3.client('s3')

    # check : whether date folder exists, if not create he folder in s3
    try:
        s3.Object(BucketName, check_folder).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print('The datefolder does not exist.Creating date folder.')
            s3_client.put_object(Bucket=BucketName, Key="archive/"+cur_date + "/")
            my_file_new = "archive/"+cur_date+"/done_" + filename
        else:
           raise
    else:
        print('The date folder exists.')
        my_file_new = "archive/"+cur_date+"/done_" + filename

    #Rename and Move file
    s3.Object(BucketName, my_file_new).copy_from(CopySource=BucketName+"/"+filepath)
    s3.Object(BucketName, filepath).delete()
    print ('File archival completed with done_ prefix in date folder.')

#funtion calls
download_file_from_s3("ayanstestbucket","active","testfile.xlsx","/Users/ayan.putatunda/Desktop/Aws/testfile_active.xlsx")
download_file_from_s3("ayanstestbucket","active2","testfile2.xlsx","/Users/ayan.putatunda/Desktop/Aws/testfile2_active.xlsx")
download_file_from_s3("ayanstestbucket","active3","testfile3.xlsx","/Users/ayan.putatunda/Desktop/Aws/testfile3_active.xlsx")
download_file_from_s3("ayanstestbucket","active4","testfile4.xlsx","/Users/ayan.putatunda/Desktop/Aws/testfile4_active.xlsx")
download_file_from_s3("ayanstestbucket","active5","testfile5.xlsx","/Users/ayan.putatunda/Desktop/Aws/testfile5_active.xlsx")