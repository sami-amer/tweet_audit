from urllib import response
import warnings
import pytest, os, logging, time
from classesv1 import TweetStream, Tweet, TweetDB, TwitterHandler
from unittest.mock import Mock, patch

formatter = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s] %(message)s")
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


class fakeTwitterHandler:
    def __init__(self, logger) -> None:
        self.logger = logger
        self.responses = [
            {
                "data": {
                    "author_id": "247334603",
                    "id": "1501685993916841991",
                    "text": "As we develop climate policy, we must recognize the disproportionate impact natural disasters &amp; inaccessible resources have on women. This week, I joined @maziehirono to intro the Women &amp; Climate Change Act to ensure the US advances equitable climate solutions that work for all. https://t.co/nbWQJXPBo3",
                },
                "includes": {
                    "users": [
                        {
                            "id": "247334603",
                            "name": "Senator Dick Durbin",
                            "username": "SenatorDurbin",
                        },
                        {
                            "id": "92186819",
                            "name": "Senator Mazie Hirono",
                            "username": "maziehirono",
                        },
                    ]
                },
                "matching_rules": [{"id": "1500677568919392257", "tag": "501"}],
            },
            {
                "data": {
                    "author_id": "1099199839",
                    "id": "1501685742355066892",
                    "text": "RT @uspirg: Did you know that gas stoves can emit air pollutants in your home at levels exceeding EPA regulations for outdoor air quality?…",
                },
                "includes": {
                    "users": [
                        {
                            "id": "1099199839",
                            "name": "Martin Heinrich",
                            "username": "MartinHeinrich",
                        },
                        {"id": "42660729", "name": "U.S. PIRG", "username": "uspirg"},
                    ]
                },
                "matching_rules": [{"id": "1500677568919392261", "tag": "505"}],
            },
        ]

    def stream(self):
        # for response_line in self.responses:
        #     time.sleep(10)
        #     # if response_line:
        #     #     json_response = json.loads(response_line)
        #     self.logger.info("output fake json")
        #     yield response_line
        while True:
            time.sleep(10)
            yield self.responses[0]


class TestHandlerMock:
    def fake_get_from_endpoint_success(self, url):
        return {
            "data": [{"id": "1234567", "value": "from:TestUser"}],
            "meta": {"sent": "2022-03-06T22:47:37.158Z", "result_count": -1},
        }

    def fake_get_from_endpoint_failure(self, url):
        return {
            "title": "Unauthorized",
            "detail": "Unauthorized",
            "type": "about:blank",
            "status": 401,
        }

    @patch.object(TwitterHandler, "get_from_endpoint", fake_get_from_endpoint_success)
    def test_get_rules_working(self):
        handler = TwitterHandler(None, log_tester)
        response = handler.get_rules()
        print(response)
        ground_truths = {
            "rules": [{"id": "1234567", "value": "from:TestUser"}],
            "rule_count": -1,
        }
        assert response[0] == ground_truths

    @patch.object(TwitterHandler, "get_from_endpoint", fake_get_from_endpoint_failure)
    def test_get_rules_broken(self):
        handler = TwitterHandler(None, log_tester)
        with pytest.warns(UserWarning, match="No Rules Found!"):
            response = handler.get_rules()
        # assert err.errisinstance(KeyError)


class TestTweetClass:
    def test_set_author_id(self):
        tweet = Tweet("testID", "testText")
        tweet.set_author_id("testAuthorID")
        assert tweet.author_id == "testAuthorID"

    def test_str_method(self):
        tweet = Tweet("testID", "testText", "testAuthorID")
        print_result = tweet.__str__()
        assert print_result == ("testID", "testAuthorID", "testText")

    def test_get_dict(self):
        tweet = Tweet("testID", "testText", "testAuthorID")
        dict_result = tweet.get_dict()

        assert dict_result == {
            "tweet_id": "testID",
            "author_id": "testAuthorID",
            "tweet_text": "testText",
        }


class TestTweetDB:
    pass
