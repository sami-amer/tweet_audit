import pytest, os
from classes import TwitterStream

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


bearer_token = os.environ.get("BEARER_TOKEN")
stream = TwitterStream(bearer_token)
class TestAPI:
    
    def test_get_user_id(self):
        # This test asserts using my twitter account
        # Uses 1 API Call
        sami_id = "1441875943136448513"
        assert stream.get_user_id("Sami_Amer_PS")["id"] == sami_id

    def test_get_user_timeline(self):
        raise NotImplementedError

    def test_rules_are_active(self):
        assert stream.get_rules()[0]

    # Other tests are trickier to test since they affect the stream
    # ? Maybe make a test stream

class TestTools:
    pass
