"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/

To set your enviornment variables in your terminal run the following line:
export 'BEARER_TOKEN'='<your_bearer_token>'
""" 

import logging, os
import pandas as pd
import tools
from classes import TwitterStream

## Logging Setup
logging.basicConfig(filename='AUDIT_LOG.log', level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(ch)



if __name__ == '__main__':
    bearer_token = os.environ.get("BEARER_TOKEN")
    stream = TwitterStream(bearer_token)
    # ids = [stream.get_user_id("Sami_Amer_PS")["id"], stream.get_user_id("h_jackson_")["id"]]
    id = "1496934334657409030"
    # stream.get_user_from_tweet(id)
    stream.connect()
    # stream.get_rules()
