# native
import logging
import os
from unittest.mock import patch

# packages
import pytest

# lib
from _deprecated.classesv1 import TwitterHandler


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


class TestHandlerAPI:
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

    # def test_get_user_from_tweet(self):
    #     # Uses 1 API Call
    #     handler = TwitterHandler(bearer_token, log_tester)
    #     tweet_id = "1443636712928915459"
    #     user_id = "1327422172818788353"
    #     assert handler.get_user_from_tweet(tweet_id) == user_id

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
