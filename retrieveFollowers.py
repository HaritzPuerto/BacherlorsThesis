from twython import Twython, TwythonRateLimitError
import MySQLdb
import re
import langid
import time # standard lib
import sys


# Create a Twython instance with your application key and access token
twitter = Twython(APP_KEY, APP_SECRET, oauth_version=1)


conn = MySQLdb.connect(host= "localhost",
                       user="haritz",
                       passwd="haritz",
                       db="NLP_TFG")
cursor = conn.cursor()

log_filename = '/home/odroid/TFG/logRetrieveFollowers.txt'

selectUsers = """SELECT id, username FROM NLP_TFG.users"""
cursor.execute(selectUsers)
# Fetch all the rows in a list of lists.
results = cursor.fetchall()
# Opening the file...
log = open(log_filename, 'a') #appending
log.write("Going to the  for")
log.write("\n")
log.close()
for row in results: #for each user
    user_num = row[0]
    username = row[1]
    # Opening the file...
    log = open(log_filename, 'a') #appending
    log.write( "Looking for followers of " + username)
    log.write("\n")
    log.close()

    followers_inserted = 0
    num_api_calls = 0
    there_are_more_followers = True
    next_cursor = -1 #It'll be used for paginating the followers retrieved
    #print "looking for followers of " + username
    while followers_inserted < 200 and there_are_more_followers:
        #we wanna insert at least 200 followers. That's our goal, but if there are not enough followers,
        #we'll see later what to do. Please note,we don't want to store a hundreds of thousend of followers
        try:
            if num_api_calls >= 14: #Twitter allows us to do just 15 api calls every 15 min
                time.sleep(900)
                followers = twitter.get_followers_list(screen_name=username, count=200,cursor=next_cursor)
                num_api_calls =1
            else:
                followers = twitter.get_followers_list(screen_name=username, count=200,cursor=next_cursor)
                #print 'I have a new list of followers'
                num_api_calls += 1
            next_cursor = followers["next_cursor"]
            #print "num followers = " + str(len(followers["users"]))
            there_are_more_followers = len(followers["users"]) != 0
        except: #I can't have their followers (private account, or another error)
            followers = {}
            followers["users"] = []
        for follower in followers["users"]:
            # Add their screen name to our followers list
            if (follower["lang"] == "en" or follower["lang"] == "en-gb")\
                    and follower["statuses_count"] >= 400:
                #Add users whoose lang official is eng (watch out with bilingual users)
                #and with at least 400 tweets. We may not use all those tweets
                user = follower["screen_name"].encode("utf-8")
                insertUserSQL = """INSERT INTO NLP_TFG.users (username, follows_to) VALUES(\"""" + user + "\",\"" + str(user_num) + "\")"
                try:
                    cursor.execute(insertUserSQL)
                    conn.commit()
                    followers_inserted += 1
                except:
                    # Opening the file...
                    log = open('/home/odroid/TFG/errors.txt', 'a') #appending
                    log.write("ERROR")
                    log.write(insertUserSQL)
                    log.write("\n")
                    log.close()
            else:
                log = open('/home/odroid/TFG/errors.txt', 'a') #appending
                log.write(follower["screen_name"].encode("utf-8") + " " + follower["lang"] + " "  + str(follower["statuses_count"]))
                log.write("\n")
                log.close()
        # Opening the file...
        log = open(log_filename, 'a') #appending
        log.write("Followers of " +  username + " have been inserted")
        log.write("\n")
log.close()