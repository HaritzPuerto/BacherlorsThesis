
from twython import Twython
import MySQLdb
import re
import langid
import time # standard lib


# Create a Twython instance with your application key and access token
twitter = Twython(APP_KEY, APP_SECRET, oauth_version=1)

num_api_calls = 0
conn = MySQLdb.connect(host= "localhost",
                       user="haritz",
                       passwd="haritz",
                       db="NLP_TFG")
cursor = conn.cursor()

selectUsers = """SELECT id, username FROM NLP_TFG.users;
"""
cursor.execute(selectUsers)
# Fetch all the rows in a list of lists.
results = cursor.fetchall()
last_tweet = 0
for row in results: #for each user
    user_num = row[0]
    username = row[1]
    print username
    num_api_calls_for_this_user = 0
    #Get user's timeline
    try:
        if num_api_calls < 15:
            user_timeline=twitter.get_user_timeline(screen_name=username, count=200)
            num_api_calls_for_this_user += 1
            #We can assume the account is not private as we could retrieve info about it before (when populating the users table)
            num_api_calls += 1
            if num_api_calls == 15:
                # Opening the file...
                log = open('/home/haritz/TFG/logTweets.txt', 'a') #appending
                log.write("Going to sleep 1")
                log.write("\n")
                log.close()
                print 'im gonna sleep 1'
                time.sleep(900) ## 15 minute rest between api calls
                print 'wake up!'
                num_api_calls = 0
        else:
            # Opening the file...
            log = open('/home/haritz/TFG/logTweets.txt', 'a')  # appending
            log.write("Going to sleep 2")
            log.write("\n")
            log.close()
            print 'im gonna sleep 2'
            time.sleep(900) ## 15 minute rest between api calls
            print 'wake up!'
            user_timeline=twitter.get_user_timeline(screen_name=username, count=200)
            num_api_calls_for_this_user += 1
            #We can assume the account is not private as we could retrieve info about it before ((when populating the users table)
            num_api_calls = 1
        print 'Inserting in DB many tweets'
        for tweet in user_timeline:
            last_tweet = tweet["id"]
            #Remove links
            cleaned_tweet = re.sub(r"http\S+", "", tweet['text'])
            #Remove RT
            cleaned_tweet = re.sub("RT @\w+: ", " ", cleaned_tweet)
            #Remove mentions
            cleaned_tweet = re.sub("@\w+", " ", cleaned_tweet)
            #Take the hashtag
            hashtags = re.findall(r"#(\w+)", tweet["text"])
            hashtag = ""
            if len(hashtags) >= 1:
                hashtag = hashtags[0]
            #Remove  '#' of the hashtags
            cleaned_tweet = re.sub(r"#(\w+)", " ", cleaned_tweet)
            #Remove strange characters. We only want ascii chars
            cleaned_tweet = re.sub('[^0-9a-zA-Z]+', ' ', cleaned_tweet)
            try:
                if langid.classify(cleaned_tweet)[0] == "en" and len(re.findall(r'\w+', cleaned_tweet)) >= 10:
                    #store only tweets in English with at least 10 words:
                    created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
                    cursorInsert = conn.cursor() #new cursor for the insert
                    cursorInsert.execute("""INSERT INTO NLP_TFG.timelines2 (tweet, user_id, date_of_tweet, hashtag, tweet_id) VALUES (\" """ + cleaned_tweet + " \"," + str(user_num) + ", '" + created_at + "' , '" + hashtag + "', " + str(tweet['id']) + ") """)
                    cursorInsert.close()
                    conn.commit()
            except:
                conn.rollback()
                # Opening the file...
                created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
                sql = """INSERT INTO NLP_TFG.timelines2 (tweet, user_id, date_of_tweet, hashtag, tweet_id) VALUES (\" """ + cleaned_tweet + " \"," + str(user_num) + ", '" + created_at + "' , '" + hashtag + "', " + str(tweet['id']) + ")  """
                log = open('/home/haritz/TFG/errorsTweets.txt', 'a') #appending
                log.write(sql)
                #log.write("""INSERT INTO NLP_TFG.timelines (tweet, user_id, tweet_id) VALUES (\" """ + cleaned_tweet + " \"," + str(user_num) + ", " + str(tweet['id']) + ") """)
                log.write("\n")
                log.close()

        #Now I have a valid last_tweet value
        inserted_tweets = 0
        too_many_attemps = False
        while num_api_calls < 15 and inserted_tweets <= 400 and not too_many_attemps: #According to Twitter API we can only make 15 calls every 15 minutes
            #we want to store 400 tweets
            #Get user's timeline
            print "num api calls = " + str(num_api_calls)
            print "num_api_calls_for_this_user = " + str(num_api_calls_for_this_user)
            try:
                if num_api_calls < 15:
                    user_timeline=twitter.get_user_timeline(screen_name=username, count=200, max_id=last_tweet-1)
                    num_api_calls_for_this_user += 1
                    #We can assume the account is not private as we could retrieve info about it before (when populating the users table)
                    num_api_calls += 1
                    if num_api_calls == 15:
                        # Opening the file...
                        log = open('/home/haritz/TFG/logTweets.txt', 'a') #appending
                        log.write("Going to sleep 3")
                        log.write("\n")
                        log.close()
                        print 'im gonna sleep 3'
                        time.sleep(900) ## 15 minute rest between api calls
                        print 'wake up!'
                        num_api_calls = 0
                else:
                    # Opening the file...
                    log = open('/home/haritz/TFG/logTweets.txt', 'a')  # appending
                    log.write("Going to sleep 4")
                    log.write("\n")
                    log.close()
                    print 'im gonna sleep 4'
                    time.sleep(900) ## 15 minute rest between api calls
                    print 'wake up!'
                    user_timeline=twitter.get_user_timeline(screen_name=username, count=200, max_id=last_tweet-1)
                    num_api_calls_for_this_user += 1
                    #We can assume the account is not private as we could retrieve info about it before ((when populating the users table)
                    num_api_calls = 1

                for tweet in user_timeline:
                    #Remove links
                    cleaned_tweet = re.sub(r"http\S+", "", tweet['text'])
                    #Remove RT
                    cleaned_tweet = re.sub("RT @\w+: ", " ", cleaned_tweet)
                    #Remove mentions
                    cleaned_tweet = re.sub("@\w+", " ", cleaned_tweet)
                    #Take the hashtag
                    hashtags = re.findall(r"#(\w+)", tweet["text"])
                    hashtag = ""
                    if len(hashtags) >= 1:
                        hashtag = hashtags[0]
                    #Remove  '#' of the hashtags
                    cleaned_tweet = re.sub(r"#(\w+)", " ", cleaned_tweet)
                    #Remove strange characters. We only want ascii chars
                    cleaned_tweet = re.sub('[^0-9a-zA-Z]+', ' ', cleaned_tweet)
                    #Take its id
                    last_tweet = tweet["id"]
                    try:
                        if langid.classify(cleaned_tweet)[0] == "en" and len(re.findall(r'\w+', cleaned_tweet)) >= 10:
                            #store tweets in English with at least 10 words:
                            created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
                            cursorInsert = conn.cursor()
                            cursorInsert.execute("""INSERT INTO NLP_TFG.timelines2 (tweet, user_id, date_of_tweet, hashtag, tweet_id) VALUES (\" """ + cleaned_tweet + " \"," + str(user_num) + ", '" + created_at + "' , '" + hashtag + "', " + str(tweet['id']) + ") """)
                            cursorInsert.close()
                            conn.commit()
                            inserted_tweets += 1
                    except:
                        conn.rollback()
                        created_at = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
                        # Opening the file...
                        log = open('/home/haritz/TFG/errorsTweets.txt', 'a') #appending
                        log.write("""INSERT INTO NLP_TFG.timelines2 (tweet, user_id, date_of_tweet, hashtag, tweet_id) VALUES (\" """ + cleaned_tweet + " \"," + str(user_num) + ", '" + created_at + "' , '" + hashtag + "', " + str(tweet['id']) + ") """)
                        log.write("\n")
                        log.close()
            except:
                # Opening the file...
                log = open('/home/haritz/TFG/errorsTweets.txt', 'a') #appending
                log.write(username + " has only one tweet")
                log.write("\n")
                log.close()
            if (num_api_calls_for_this_user >= 3 and inserted_tweets < 100) or  num_api_calls_for_this_user >= 10:
                too_many_attemps = True
                #First condition: I've retrieved 600 tweets ans I have only inserted less than 100
                #skip this user
                #Second condition: I've retrieved 2000 tweets but I could find 400 valid tweets so skip this user
                #I don't want to waste too much time in this user
        # Opening the file...
        log = open('/home/haritz/TFG/logTweets.txt', 'a') #appending
        log.write(username + ' s tweets inserted')
        log.write("\n")
        log.close()
    except:
        # Opening the file...
        log = open('/home/haritz/TFG/errorsTweets.txt', 'a') #appending
        log.write("Error inserting " + username)
        log.write("\n")
        log.close()

log = open('/home/haritz/TFG/logTweets.txt', 'a') #appending
log.write('FINISHED')
log.write("\n")
log.close()
conn.close()

#followers = twitter.get_followers_ids(screen_name = "MaraIsabelMart6", count=500)

#for follower_id in followers["ids"]:
#    user = twitter.show_user(user_id=follower_id)
#    print user["screen_name"]