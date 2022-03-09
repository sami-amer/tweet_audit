"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/

To set your enviornment variables in your terminal run the following line:
export 'BEARER_TOKEN'='<your_bearer_token>'
"""

import logging, os
from classes import TweetStream


if __name__ == "__main__":
    # ! ADD MORE ERROR CATCHES!
    bearer_token = os.environ.get("BEARER_TOKEN")
    stream = TweetStream(bearer_token, "test.db")
    stream.run()
