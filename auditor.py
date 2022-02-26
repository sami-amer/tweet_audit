"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/

To set your enviornment variables in your terminal run the following line:
export 'BEARER_TOKEN'='<your_bearer_token>'
""" 

import logging, os
from multiprocessing import Process, Pipe
import pandas as pd
import tools
from classes import TwitterStream, Tweet, TweetDB


## Logging Setup
logging.basicConfig(filename='AUDIT_LOG.log', level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(ch)

# ! IMPLEMENT PIPE


if __name__ == '__main__':
    bearer_token = os.environ.get("BEARER_TOKEN")
    storage = {}
    db = TweetDB(storage)
    stream = TwitterStream(bearer_token, db)


    api_connect = Process(target=stream.connect())
    db_connect = Process(target = db.connect_to_queue())

    api_connect.start()
    db_connect.start()



