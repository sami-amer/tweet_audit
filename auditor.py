"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/

To set your enviornment variables in your terminal run the following line:
export 'BEARER_TOKEN'='<your_bearer_token>'
""" 

import logging, os
from classes import TweetStream


# ## Logging Setup
# logging.basicConfig(filename='AUDIT_LOG.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
# logger = logging.getLogger()
# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
# logger.addHandler(ch)




if __name__ == '__main__':
    # ! ADD MORE ERROR CATCHES!
    bearer_token = os.environ.get("BEARER_TOKEN")
    stream = TweetStream(bearer_token,"test.db")
    stream.run()

