import tweepy
import random


# Override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    tweets = []
    tags = {}
    sample_size = 150
    sequence_number = 0

    def on_status(self, tweet):
        hashtags = tweet.entities["hashtags"]
        if len(hashtags) > 0:
            self.sequence_number += 1
            # If there is still enough room to add tweet, keep adding
            if self.sequence_number < self.sample_size:
                self.tweets.append(tweet)
                for i in hashtags:
                    tag = i['text']
                    if tag not in self.tags.keys():
                        self.tags[tag] = 0
                    self.tags[tag] += 1
            # Otherwise, randomize an integer to decide whether we want to replace with an existing tweet with the
            #   upcoming one. If we randomize 0, replace. Else, discard the upcoming tweet
            else:
                prob = random.randint(0, self.sample_size - 1)
                # Replace a tweet with the upcoming tweet
                if prob == 0:
                    # Remove the unfortunate tweet and its tags
                    index = random.randint(0, self.sample_size - 1)
                    removed_tweet = self.tweets[index]
                    removed_tag = removed_tweet.entities['hashtags']
                    for i in removed_tag:
                        h = i['text']
                        self.tags[h] -= 1
                        if self.tags[h] == 0:
                            self.tags.pop(h)
                    self.tweets[index] = tweet

                    # Add the incoming tweet and its tags to the list
                    for i in hashtags:
                        tag = i['text']
                        if tag not in self.tags.keys():
                            self.tags[tag] = 0
                        self.tags[tag] += 1

            result = sorted(self.tags.items(), key=lambda kv: (-kv[1], kv[0]))
            print(f"The number of tweets with tags from the beginning: {self.sequence_number}")
            i, previous_count = 0, -1
            for tag, count in result:
                if i >= 5 and previous_count > count:
                    break
                print(f"{tag}: {count}")
                i += 1
                previous_count = count

    def on_error(self, status_code):
        if status_code == 420:
            return False


api_key = "wLB7vVATOOVdmXLUF5vBewDAW"
api_key_secret = "dmtitr96AzadZYFVzwXtVm09E4t1yexpETcsfFsfMXwC7Q9XDX"
access_token = "1199119887865528321-GFf6mVrjfX2LLfr2LPwiyELCoSOTKt"
access_token_secret = "VA4Pzrr5TPnMhp07D7rU5PsfkJjrgw4R3TS5schKs4563"

auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
myStream.filter(track=["BLACKPINK"])
