import boto3
import json
from TwitterAPI import TwitterAPI, TwitterConnectionError, TwitterRequestError

kinesis=boto3.client('kinesis',
                     aws_access_key_id='AKIAI42RFARRUH224INQ',
                     aws_secret_access_key='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',
                     region_name='us-east-1'
                     )

twitter=TwitterAPI(
                   'QRhY8WsSOsfNh5F0RoH2rztob','uzaBmHQiAbDHrr4ofu2WBxolodArz215Ip1YcfHTrccvvxTHK3',
                   '825228334007148544-Fxo3wfKWbCUBgY0S3RiprexqVerxaop','FovlNX1hMqJNGhpzg6yDJn9fYNVlitk6jOs5fkgNGwrlt'
                  )

usersToFollow= [82221567926100480]
params={ 'follow': usersToFollow, 'extended': True }
stream=twitter.request('statuses/filter',params)

for item in stream:
    kinesis.put_record(StreamName='twitter',
                       Data=json.dumps(item),PartitionKey="filler")
