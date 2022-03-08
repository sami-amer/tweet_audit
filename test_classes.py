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