import tweepy
from tweepy import OAuthHandler
from accessConfig import *
import json
import codecs
import boto3
import logging
import time
 
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth)
queryHashtag = 'DonaldTrump'
kinesis = boto3.client("kinesis")
shard_id = 'shardId-000000000000' #only one shard
shard_it = kinesis.get_shard_iterator(StreamName="twitter", ShardId=shard_id, ShardIteratorType="LATEST")["ShardIterator"]

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('hashtags')

def process_or_store(tweet):
 #print(json.dumps(tweet))
    #f = codecs.open('tweetDump.json', 'a','utf-8') #writing to local file.
    try:

        while 1==1:
	out = kinesis.get_records(ShardIterator=shard_it, Limit=100)
	for record in out['Records']:
		if 'entities' in json.loads(record['Data']):
			htags = json.loads(record['Data'])['entities']['hashtags']
			if htags:
 				for ht in htags:
					htag = ht['text']	
					checkItemExists = table.get_item(
 					       Key={
                					'hashtag':htag
        					}
					)					
					if 'Item' in checkItemExists:
						response = table.update_item(
							Key={
								'hashtag': htag 
							},
							UpdateExpression="set htCount  = htCount + :val",
							ConditionExpression="attribute_exists(hashtag)",
							ExpressionAttributeValues={
								':val': decimal.Decimal(1) 	
							},
							ReturnValues="UPDATED_NEW"
						)
					else: 
                                		response = table.update_item(
                                        		Key={
                                                		'hashtag': htag
                                        		},
                                        		UpdateExpression="set htCount = :val",
                                        		ExpressionAttributeValues={
                                                		':val': decimal.Decimal(1)
                                        		},
                                        		ReturnValues="UPDATED_NEW"
                                		)    
	shard_it = out["NextShardIterator"]
	time.sleep(1.0)




        response = firehose_client.put_record(
            DeliveryStreamName='bhargav-twitter-data-stream',
            Record={
                'Data': json.dumps(tweet, ensure_ascii=False, encoding="utf-8")+'\n'
            }
        )
        logging.info(response)
    except Exception:
        logging.exception("Problem pushing to dyanmodb")
    #f.write(json.dumps(tweet, ensure_ascii=False, encoding="utf-8")+'\n')
    #f.close()

firehose_client = boto3.client('firehose', region_name="us-east-1")
LOG_FILENAME = '/tmp/bhargav-twitter-data-stream.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

def main():
   for tweet in tweepy.Cursor(api.search, q=queryHashtag).items(10):
   	process_or_store(tweet._json)

startTime=time.time()
while True:
	if __name__ == "__main__":
	    main()
	time.sleep(1800.0 - time.time() % 60)
