import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.social_net


def get_total_user_amount():
    print (len(db.tweets.distinct('user')))


def get_most_linking_users():
    pipeline = [
        {'$match':{'text':{'$regex':"@\w+"}}},
        {'$addFields': {"mentions":1}},
        {'$group':{"_id":"$user", "mentions":{'$sum':1}}},
        {'$sort':{"mentions":-1}},
        {'$limit':10}]
    cursorlist = db.tweets.aggregate(pipeline)
    for cursor in cursorlist:
        print(cursor)


def get_most_mentioned_users():
    pipeline = [
        {'$addFields': {'words': {'$split': ['$text', ' ']}}},  # split text
        {'$unwind': "$words"},  # reconstruct an array of words
        {'$match': {'words': {'$regex': "@\w+", '$options': 'm'}}}, # match the @ from the words list
        {'$group': {'_id': "$words", 'total': {'$sum': 1}}},
        {'$sort': {'total': -1}},  # sort the total
        {'$limit': 5},
    ]
    tweets = db.tweets.aggregate(pipeline)
    print (list(tweets))


def get_most_active_user():
    pipeline = [
        {'$group': {'_id': "$user", 'total': {'$sum': 1}}},
        {'$sort': {'total': -1}},
        {'$limit': 10},
    ]
    users = db.tweets.aggregate(pipeline)
    for user in users:
        print(user)


def get_most_negative_tweets():
    
    pipeline = [
        {'$match':{'text': {'$regex':'pissed|mad|angry|sad|furious|outraged','$options':'g'}}},
        {'$group':{'_id':"$user", 'emotion': {'$avg':"$polarity"}, 'total_negative_tweets': {'$sum': 1}}},
        {'$sort':{ 'emotion': 1, 'total_negative_tweets':-1}},
        {'$limit':5}
    ]
    negativeUser = db.tweets.aggregate(pipeline)
    for negUser in negativeUser:
        print(negUser)


def get_most_positive_tweets():
    pipeline = [
        {'$match':{'text': {'$regex':'happy|excited|great|amazing|love|enticed','$options':'g'}}},
        {'$group':{'_id':"$user", 'emotion': {'$avg':"$polarity"},'total_positive_tweets': {'$sum': 1}}},
        {'$sort':{ 'emotion': -1, 'total_positive_tweets':-1}},
        {'$limit':5}
    ]
    positiveUser = db.tweets.aggregate(pipeline)
    for posUser in positiveUser:
        print(posUser)


print('How many Twitter users are in the database?')
get_total_user_amount()
print("------")
print('Top 10 users link the most to other Twitter users?')
get_most_linking_users()
print("------")
print('Top 5 most mentioned Twitter users? ')
get_most_mentioned_users()
print("------")
print('Top 10 most active Twitter users are: ')
get_most_active_user()
print("------")
print('Top 5 users tweeting word "pissed|mad|angry|sad|furious|outraged": ')
get_most_negative_tweets()
print("------")
print ('Top 5 users tweeting word "happy|excited|great|amazing|love|enticed": ')
get_most_positive_tweets()
