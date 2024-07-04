import datetime
import time
import random
import json
from boto import kinesis


#client = boto3.client('kinesis', region_name='us-east-1')
partition_key ='partitionkey'

kinesis = kinesis.connect_to_region('us-east-1')

while True:
        time_now = datetime.datetime.now()
        print(time_now)
        data = {
                'time':str(time_now),
                'version':'V_98',
                'model': random.choice(['520D', '720D']),
                'Gender': random.choice([0, 1]),
                'age': random.randint(15,65),
                'heart_bpm': random.randint(60,100),
                'cal': random.randrange(500,3500)
        }
        random_user = json.dumps(data)
        print(random_user)
        k= kinesis.put_record(
                'db-connect',
                random_user,
                partition_key)
        print(k)
        time.sleep(random.uniform(0, 1))


