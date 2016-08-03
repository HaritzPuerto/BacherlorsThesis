import MySQLdb


conn = MySQLdb.connect(host= "localhost",
                       user="haritz",
                       passwd="haritz",
                       db="NLP_TFG")
cursor = conn.cursor()

concat = "SET group_concat_max_len = 18446744073709551615;"
cursor.execute(concat)


selectTweets = """SELECT GROUP_CONCAT(tweet SEPARATOR ' '), hashtag
FROM NLP_TFG.timelines2 t
WHERE t.user_id = 3122 and hashtag != ''
GROUP BY hashtag, user_id;
"""

cursor = conn.cursor()
cursor.execute(selectTweets)
# Fetch all the rows in a list of lists.
results = cursor.fetchall()
cnt = 1
for tweet in results: #for each user
	nameOfDoc = tweet[1]
	doc = open('/home/haritz/TFG/nytimes_hashtags2/' + nameOfDoc+ '.txt', 'a') #appending
	doc.write(tweet[0])
	doc.write(" ")
	doc.close()


# tweets without hashtag

selectTweetsWithoutHashtag = """SELECT tweet
	FROM NLP_TFG.timelines2 t
	WHERE t.user_id = 3122 and hashtag = '';"""

noHashtagCursor = conn.cursor()
noHashtagCursor.execute(selectTweetsWithoutHashtag)
# Fetch all the rows in a list of lists.
results = noHashtagCursor.fetchall()
cnt = 1
for tweet in results: #for each user
	nameOfDoc = str(cnt)
	cnt += 1
	doc = open('/home/haritz/TFG/nytimes_hashtags2/' + nameOfDoc + '.txt', 'a') #appending
	doc.write(tweet[0])
	doc.write(" ")
	doc.close()