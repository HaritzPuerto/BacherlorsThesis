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
WHERE t.user_id = 3123
GROUP BY hashtag, user_id;
"""

cursor = conn.cursor()
cursor.execute(selectTweets)
# Fetch all the rows in a list of lists.
results = cursor.fetchall()
for tweet in results: #for each user
	nameOfDoc = tweet[1]
	if tweet[1] == ' ' or tweet[1] == '':
		nameOfDoc = 'noHashtag'
	doc = open('/home/haritz/TFG/obama/' + nameOfDoc+ '.txt', 'a') #appending
	doc.write(tweet[0])
	doc.write(" ")
	doc.close()
