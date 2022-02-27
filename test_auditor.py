import pytest, os, logging
from classes import TweetStream, Tweet, TweetDB, TwitterHandler

formatter = logging.Formatter('%(asctime)s [%(name)s][%(levelname)s] %(message)s')
log_tester = logging.getLogger("Tester")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

fh_tester = logging.FileHandler("logs/TESTER_LOG.log")
fh_tester.setFormatter(formatter)
log_tester.addHandler(fh_tester)
log_tester.addHandler(ch)

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

class TestHandler:
    # Uses 4 API Calls total
    def test_get_user_id(self):
        # This test asserts using my twitter account
        # Uses 1 API Call
        handler = TwitterHandler(bearer_token, log_tester)
        sami_id = "1441875943136448513"
        assert handler.get_user_id("Sami_Amer_PS")["id"] == sami_id

    def test_rules_are_active(self):
        # Uses 1 API Call
        handler = TwitterHandler(bearer_token, log_tester)
        assert handler.get_rules()[0]

    def test_get_user_from_tweet(self):
        # Uses 1 API Call
        handler = TwitterHandler(bearer_token, log_tester)
        tweet_id = "1443636712928915459"
        user_id = "1327422172818788353"
        assert handler.get_user_from_tweet(tweet_id) == user_id 


    # Other tests are trickier to test since they affect the stream
    # ? Maybe make a test stream

class TestTweetClass:
    
    def test_set_author_id(self):
        testId = "7777777"
        tweet = Tweet("123456","some Text")
        tweet.set_author_id(testId)
        assert tweet.author_id == testId
