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

kinesis = boto3.client('kinesis')
shard_id = "shardId-000000000000" #only one shard!
pre_shard_it = kinesis.get_shard_iterator(StreamName="twitter1", ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

def process_or_store(tweet):
    #print(json.dumps(tweet))
    #f = codecs.open('tweetDump.json', 'a','utf-8') #writing to local file.
    try:
        response = kinesis.put_record(StreamName="twitter1", Data=json.dumps(tweet, ensure_ascii=False, encoding="utf-8")+'\n', PartitionKey="filler")
        logging.info(response)
    except Exception:
        logging.exception("Problem pushing to kinesis")

LOG_FILENAME = '/tmp/swetha-twitter-data-stream.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
def main():
   for tweet in tweepy.Cursor(api.search, q=queryHashtag).items(10):
        process_or_store(tweet._json)

