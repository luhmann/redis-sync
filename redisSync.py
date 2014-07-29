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
partialIndicator = '_partial'
redisPartialFields = ['url']
baseDir = os.path.dirname(os.path.realpath(__file__))

# Parses the keys.txt file and writes url
def read_redis_keys():
    keyListFilename = os.path.join(baseDir, 'keys.txt')
    return  [line.strip() for line in open(keyListFilename, 'r')]

def parse_for_partials(json_string):
    results = []
    #print json_string
    def _check_url_value(json_obj):
        try:
            for key in redisPartialFields:
                if isinstance(json_obj[key], basestring):
                    url = json_obj[key]
                    if partialIndicator in url:
                        results.append(url)
        except KeyError: pass
        return json_obj
    # check for empty string
    if json_string:
        obj = json.loads(json_string, object_hook=_check_url_value)
    return results

def sync_redis_keys(keys):
    referenced_keys = []

    for key in keys:
        if key not in written_keys:
            content = redisSrcHandle.get(redisKeyPrefix + key)
            # print content
            referenced_keys = referenced_keys + parse_for_partials(content)
            print 'Writing key ' + key
            # redisDestHandle.set(redisKeyPrefix + key, content)
            written_keys.append(key)

    if referenced_keys:
        sync_redis_keys(referenced_keys)

# Read in redis-keys
written_keys = []
keys = read_redis_keys()
print 'Redis Keys: \n' + '\n'.join(keys)
sync_redis_keys(keys)

