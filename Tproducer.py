


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from confluent_kafka import Producer

import json


 
# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler('API KEY', 'API SECRET KEY')
        auth.set_access_token("Access token", "Access token secret")
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(track=hash_tag_list)



# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    # def acked(err,msg):
    #     """ Called once for each message produced to indicate delivery result.
    #     Triggered by poll() or flush(). """
    # if err is not None:
    #     print('Message delivery failed: {}'.format(err))
    # else:
    #     print('Message delivered to topic: {} partition: [{}] offset: {}'.format(msg.topic(), msg.partition(), msg.offset())) 

    def on_data(self, data):
        try:
            p.poll(0)
            # p.produce('registered_users', data.encode('utf-8'), callback=acked)
            print("producing data")
            p.produce('registered_users', data.encode('utf-8'))
            p.flush()
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

  


 
if __name__ == '__main__':
 
    # Authenticate using config.py and connect to Twitter Streaming API.
    hash_tag_list = ["India"]
    fetched_tweets_filename = "tweets.txt"
    p = Producer({'bootstrap.servers': '192.168.0.3:9092',
                    'enable.idempotence': True,
                    'acks': 'all',
                    'linger.ms': 20,
                    'batch.size': 32*1024,
                    'compression.type': 'snappy'
                })
    print()
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)






