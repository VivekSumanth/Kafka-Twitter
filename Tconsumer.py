from datetime import datetime
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer
import re,json

es = Elasticsearch()

def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition data : " + str(partitions))

def senddata(s,i):
    res = es.index(index="test-index", id=i, body=s)
    print("tweet stored in elastic search: " +res['result'],i)

def extractId(tweet):
    l = json.loads(tweet.decode('utf-8'))
    print('tweet extracted:' +l.get('id_str'))
    return l.get('id_str')

c = Consumer({
    'bootstrap.servers': '192.168.0.3:9092"',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'on_commit': commit_completed
})
c.subscribe(['registered_users'])

print('consumer up ')
MIN_COMMIT_COUNT = 10
msg_count = 0

while True:

    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    else:
        tweetid = extractId(msg.value())
        tweet = ((msg.value().decode('utf-8')))
        # print('Received message: {}'.format(msg.value().decode('utf-8')))
        msg_count += 1 

        if msg_count % MIN_COMMIT_COUNT == 0:
            c.commit(asynchronous=True)
            print("last committed at" + tweetid + " tweet")

        senddata(tweet,tweetid)

c.close()







