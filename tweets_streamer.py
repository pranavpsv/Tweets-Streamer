import tweepy
import datetime
import time
import pandas as pd\
# Credentials is the module that has the twitter authentication details.
import credentials
consumer_key = credentials.consumer_key
consumer_secret = credentials.consumer_secret
access_token = credentials.access_token
access_token_secret = credentials.access_token_secret

df = pd.DataFrame(columns = ["created_at", "id_str","text"])
tweets_list = []
class StreamListener(tweepy.StreamListener):

    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        self.IST = datetime.timedelta(hours = 5, minutes = 30)

        super(StreamListener, self).__init__()
        
    def on_status(self, status, tweet_mode='extended'):
        if (time.time() - self.start_time) < self.limit:
            if (not status.retweeted and 
                "RT" not in status.text[:2]):
                text = status.text
                id_str = status.id_str
                created = (status.created_at + self.IST).strftime("%d-%b-%Y (%H:%M:%S)")
                tweets_count = len(tweets_list)
                
                print(tweets_count)
                
                try: 
                    tweet = status.extended_tweet['full_text']
                    tweets_list.append(tweet)
                    df.loc[tweets_count] =  [created, id_str, tweet]
                except:
                    tweet = text
                    tweets_list.append(tweet)
                    df.loc[tweets_count] =  [created, id_str, tweet]
        else:
            return False


    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False


# keywords_list is the list of keywords that are filtered in the streamed tweets.
keywords_list = ["BBC News", "PM Modi", "Economy"]

# main() runs the tweepy streamer. The parameters are:
#  duration_min: Duration for which streamer should run
#  keywords_list: List of keywords to stream for.
def main(duration_min = 2, keywords_list = keywords_list):
    start = time.time()
    duration_secs = duration_min * 60
    end_time = start + duration_secs
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    stream_listener = StreamListener(duration_secs)
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=keywords_list)
    df.to_csv("tweets_streamed.csv") # converts all tweets streamed 
            
print(main())