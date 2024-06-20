from datetime import datetime
from ntscraper import Nitter
import pandas as pd
import s3fs

def xtweet_etl():
    
    #Get the latest tweets from a user (elonmusk)
    tweets = Nitter().get_tweets("elonmusk", mode='user', number=100)

    #refining the data and saving as csv
    list = []
    for tweet in tweets["tweets"]:
        refined_tweet = {"user": tweet["user"]["name"],
                        "text" :(tweet["text"]),
                        "retweet_count" : tweet["stats"]["retweets"],
                        "Likes_count"   : tweet["stats"]["likes"],
                        "comment_count" : tweet["stats"]["comments"],
                        'created_at' : tweet["date"]}
        list.append(refined_tweet)  
    df = pd.DataFrame(list)
    df = df[df["user"] == "Elon Musk"]
    df.to_csv('s3://ramzal-xtweet-bucket/refined_tweets.csv')   
