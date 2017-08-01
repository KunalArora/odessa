import redis
import os
import sys
import json

# For seeding elasticache data locally
path = os.path.dirname(sys.argv[0])
if not path:
    path = '.'
with open(f'{path}/device_subscriptions.json') as data_file:
    device_subscriptions = json.load(data_file)
r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.flushall()

for device in device_subscriptions:
    r.hmset(f'device_subscriptions:{device["id"]}',
            {"oids": device["oids"],
             "status": device["status"],
             "message": device["message"],
             "created_at": device["created_at"],
             "updated_at": device["updated_at"]
             }
            )
