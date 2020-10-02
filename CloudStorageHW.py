import boto3
s3 = boto3.resource('s3', aws_access_key_id='AKIA6PLAKE2FTHGXHIXT', aws_secret_access_key='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')
try:
	s3.create_bucket(Bucket='datacont-connor', CreateBucketConfiguration={
	'LocationConstraint': 'us-west-2'})
except:
	print ("this may already exist")
bucket = s3.Bucket("datacont-connor")
bucket.Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
 region_name='us-west-2',
 aws_access_key_id='AKIA6PLAKE2FTHGXHIXT',
 aws_secret_access_key='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'
)

try:
 table = dyndb.create_table(
 TableName='DataTable',
 KeySchema=[
 {
 'AttributeName': 'PartitionKey',
 'KeyType': 'HASH'
 },
 {
 'AttributeName': 'RowKey',
 'KeyType': 'RANGE'
 }
 ],
 AttributeDefinitions=[
 {
 'AttributeName': 'PartitionKey',
 'AttributeType': 'S'
 },
 {
 'AttributeName': 'RowKey',
 'AttributeType': 'S'
 },
 ],
 ProvisionedThroughput={
 'ReadCapacityUnits': 5,
 'WriteCapacityUnits': 5
 }
 )
except:
 table = dyndb.Table("DataTable")


table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)
import csv
with open(r'c:\Users\conno\OneDrive\Documents\PITT FILES\CS 1660 - Cloud Computing\Homework 2\CloudStorageHW\experiments.csv', 'r') as csvfile:
 csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
 for item in csvf:
        print (item)
        body = open(r'c:\Users\conno\OneDrive\Documents\PITT FILES\CS 1660 - Cloud Computing\Homework 2\CloudStorageHW\datafiles\\'+item[3], 'rb')
        s3.Object('datacont-connor', item[3]).put(Body=body )
        md = s3.Object('datacont-connor', item[3]).Acl().put(ACL='public-read')
        url = " https://s3-us-west-2.amazonaws.com/datacont-connor/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1], 'description' : item[4], 'date' : item[2], 'url':url}
        try:
                table.put_item(Item=metadata_item)
        except:
                print ("item may already be there or another failure")
 
