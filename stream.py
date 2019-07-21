import boto3
import json
from TwitterAPI import TwitterAPI, TwitterConnectionError, TwitterRequestError

kinesis=boto3.client('kinesis',
                     aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxx',
                     aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxx',
                     region_name='us-east-1'
                     )

twitter=TwitterAPI(
                   'xxxxxxxxxx','xxxx',
                   'xxxxxxxxxx','xxxxxxxxxxxxxxxxxxxxxxxx'
                  )

usersToFollow= [82221567926100480]
params={ 'follow': usersToFollow, 'extended': True }
stream=twitter.request('statuses/filter',params)

for item in stream:
    kinesis.put_record(StreamName='twitter',
                       Data=json.dumps(item),PartitionKey="filler")
