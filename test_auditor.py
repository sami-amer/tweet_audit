from urllib import response
import warnings
import pytest, os, logging
from classes import TweetStream, Tweet, TweetDB, TwitterHandler
from unittest.mock import Mock, patch

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

class Test_Handler_API:
    # Uses 3 API Calls total
    # def test_get_user_id(self):
    #     # This test asserts using my twitter account
    #     # Uses 1 API Call
    #     handler = TwitterHandler(bearer_token, log_tester)
    #     sami_id = "1441875943136448513"
    #     assert handler.get_user_id("Sami_Amer_PS")["id"] == sami_id

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

    def test_get_from_endpoint(self):
        handler = TwitterHandler(bearer_token, log_tester)
        user_fields = "user.fields=id,verified,description,created_at"
        username = f"usernames=Sami_Amer_PS"
        url = "https://api.twitter.com/2/users/by?{}&{}".format(username, user_fields)
        try:
            response = handler.get_from_endpoint(url)
        except Exception as err:
            pytest.fail("{}".format(err))



    # Other tests are trickier to test since they affect the stream
    # ? Maybe make a test stream

class Test_handler_Mock:

    def fake_get_from_endpoint_success(self,url):
        return {"data": [{"id": "1234567", "value": "from:TestUser"}], "meta": {"sent": "2022-03-06T22:47:37.158Z", "result_count": -1}}

    def fake_get_from_endpoint_failure(self,url):
        return {"title":"Unauthorized","detail":"Unauthorized","type":"about:blank","status":401}

    @patch.object(TwitterHandler, 'get_from_endpoint', fake_get_from_endpoint_success)
    def test_get_rules_working(self):
        handler = TwitterHandler(None, log_tester)
        response = handler.get_rules()
        print(response)
        ground_truths = ({"rules":[{"id": "1234567", "value": "from:TestUser"}],"rule_count":-1})
        assert response[0] == ground_truths

    @patch.object(TwitterHandler, 'get_from_endpoint', fake_get_from_endpoint_failure)
    def test_get_rules_broken(self):
        handler = TwitterHandler(None,log_tester)
        with pytest.warns(UserWarning, match = "No Rules Found!"):
            response = handler.get_rules()
        # assert err.errisinstance(KeyError)

class Test_tweet_class:

    def test_set_author_id(self):
        tweet=  Tweet("testID","testText")
        tweet.set_author_id("testAuthorID")
        assert tweet.author_id == "testAuthorID"

    def test_str_method(self):
        tweet=  Tweet("testID","testText","testAuthorID")
        print_result = tweet.__str__()
        assert print_result == ("testID","testAuthorID","testText")

    def test_get_dict(self):
        tweet=  Tweet("testID","testText","testAuthorID")
        dict_result = tweet.get_dict()

        assert dict_result == {"tweet_id": "testID", "author_id":"testAuthorID", "tweet_text":"testText"}

class Test_TweetDB:
    pass