"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/

To set your enviornment variables in your terminal run the following line:
export 'BEARER_TOKEN'='<your_bearer_token>'
"""

# native
import atexit
import os

# lib
from classes import POSTGRES_ARGS, SQLLITE_ARGS, MAC_ARGS
from classes.classesv2 import TweetStream



if __name__ == "__main__":
    # ! ADD MORE ERROR CATCHES!
    bearer_token = os.environ.get("BEARER_TOKEN")
    # stream = TweetStream(bearer_token, "test.db")
    # postgres_args = {"host": "localhost", "dbname": "template1", "user": "postgres"}
    
    stream = TweetStream(bearer_token, MAC_ARGS)
    stream.run()
