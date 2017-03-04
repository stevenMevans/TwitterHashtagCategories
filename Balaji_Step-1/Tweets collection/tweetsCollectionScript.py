from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


access_token = "1381068211-8bAr4ZRVi2QiUjciL0pOsm68iVf7gx5FMXGhAA7"
access_token_secret = "Mg8g40W6jG8pzRoH1hxtYgYdO7nsoETq3dD0djkUs0Of8"
consumer_key = "6pY1sfhc7cEfbCLxR62qMH4Pa"
consumer_secret = "5IHR3H4EJQCL5dPx5Z9w6vcAVPwiQO9oKXajwF0SpSSs4aNQ2J"
class StdOutListener(StreamListener):

    def on_data(self, data):
        with open('fetched_tweets_output.json','a') as tf:
            tf.write(data)
            print(data)
            return True
    
if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())
    stream.filter(track=['trump','cnn','oscar', 'superbowl', 'tech', 'russia', 'climatechange', 'food', 'shopping', 'iphone', 'mac', 'billgates', 'TeamKC', 'WhatILookForInAFriend', 'ban', 'cup', 'programming'])
