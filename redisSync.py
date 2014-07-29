import argparse
import redis
import os
import requests
import json

parser = argparse.ArgumentParser(description='Takes a list of redis keys')
parser.add_argument('--redisSrc', dest='redisSrc', default='10.228.39.180', help='The host we get the redis data from')
parser.add_argument('--redisDest', dest='redisDest', default='frontend.vag-devfe-01.vag-jfd.magic-technik.de', help='The host we write the redis data to')
args = parser.parse_args()

# map arguments
redisSrc = args.redisSrc
redisDest = args.redisDest

redisSrcHandle = redis.StrictRedis(host=redisSrc, port=6379, db=0)
redisDestHandle = redis.StrictRedis(host=redisDest, port=6379, db=0)
redisKeyPrefix = 'content:v1:de:de:live:'
baseDir = os.path.dirname(os.path.realpath(__file__))

# Parses the keys.txt file and writes url
def readRedisKeys():
    keyListFilename = os.path.join(baseDir, 'keys.txt')
    return  [line.strip() for line in open(keyListFilename, 'r')]

# Read in redis-keys
keys = readRedisKeys()
print 'Redis Keys: \n' + '\n'.join(keys)

for key in keys:
    content = redisSrcHandle.get(redisKeyPrefix + key)
    print 'Writing key ' + key
    redisDestHandle.set(redisKeyPrefix + key, content)

