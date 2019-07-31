    
import json
import redis
import sys

filename = '1M_test_tweets.json'
client = redis.StrictRedis(host='localhost', port=6379, db=0)
collection = sys.argv[1] # aidr
counter = 0
limit = 11181

with open(filename) as file:
    for line in file:
        data = json.loads(line)
        client.lpush(collection, json.dumps(data))
        counter += 1
        if counter >= limit:
            break