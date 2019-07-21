import boto3

client = boto3.client('kinesis',region_name='us-east-1')
response = client.create_stream(
   StreamName='twitter1',
   ShardCount=1
)
