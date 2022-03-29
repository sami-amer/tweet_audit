# native
from collections import defaultdict
from http.client import responses
import json
import logging
import os
import requests
import time
import warnings


# packages
import redis


# lib

"""
To set your enviornment variables in your terminal run the following line:
export 'REDIS_PASS'='<your_redis_password>'

export 'REDIS_HOST'='<your_redis_host>'
"""


# redis_host = os.environ.get("REDIS_HOST")

# redis_pass = os.environ.get("REDIS_PASS")

redis_url = os.environ.get("REDIS_URL")


class RedisHandler:
    """
    Python Object to control TwitterAPIv2 stream
    """

    def __init__(self, bearer_token: str):
        # To set your enviornment variables in your terminal run the following line:
        # export 'BEARER_TOKEN'='<your_bearer_token>'
        self.bearer_token = bearer_token
        self.logger = self.create_logger()
        self.r = redis.from_url(redis_url)
        # self.r = redis.from_url("redis://localhost:6379", health_check_interval=30)

        if self.r.ping():
            self.logger.info("PONG")

    @staticmethod
    def create_logger():
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s][%(levelname)s] %(message)s"
        )
        logging.basicConfig(
            level=logging.DEBUG,
            filename="logs/ROOT_LOG.log",
            format="%(asctime)s [%(name)s][%(levelname)s] %(message)s",
        )

        # logging.basicConfig(level=logging.DEBUG)

        log_redis = logging.getLogger("Redis")

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        fh_redis = logging.FileHandler("logs/HANDLER_LOG.log", mode="w+")
        fh_redis.setFormatter(formatter)
        # fh_root = logging.FileHandler("logs/ROOT_LOG.log", mode="w+")
        # fh_root.setFormatter(formatter)

        log_redis.addHandler(fh_redis)
        log_redis.addHandler(ch)

        # log_root.addHandler(fh_root)
        # log_root.addHandler(ch)

        return log_redis

    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserLookupPython"
        return r

    def get_from_endpoint(self, url: str, params=None) -> dict:
        """
        Connnects to the Twitter API using the given url and params

        Arguments:
            url     (str): the url genereated by a function
            params  (str): parameters that need to be passed with the url [optional]
        """
        response = requests.request("GET", url, auth=self.bearer_oauth, params=params)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def post_to_endpoint(self, url: str, payload: dict) -> dict:
        response = requests.post(url, auth=self.bearer_oauth, json=payload)
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        return response

    def get_rules(self):
        """
        Gets the current deployed rules on the stream associated with the current BEARER_TOKEN
        """
        url = "https://api.twitter.com/2/tweets/search/stream/rules"
        response = self.get_from_endpoint(url)
        self.logger.debug(f"Rule Get Response: {json.dumps(response)}")
        try:
            data = response["data"]
            meta = response["meta"]
            info = {"rules": data, "rule_count": meta["result_count"]}
            self.logger.info(info)
            return info, response
        except KeyError:
            self.logger.warning("No Rules Found!")
            warnings.warn("No Rules Found!")
            return None, response

    def delete_all_rules(self, rules_response: json) -> None:
        """
        Deletes all the rules on the stream associated with the current BEARER_TOKEN
        """
        if rules_response is None or "data" not in rules_response:
            return None

        ids = list(map(lambda rule: rule["id"], rules_response["data"]))
        payload = {"delete": {"ids": ids}}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"
        response = self.post_to_endpoint(url, payload)

        try:
            self.logger.debug(f"Rule Deletion Response: {json.dumps(response)}")
        except:
            self.logger.debug(f"Rule Deletion Response: {response}")

    def delete_rules(self, ids: list) -> None:
        """
        Deletes specific rules on the stream associated with the current BEARER_TOKEN

        Arguments:
            ids (list):
        """
        payload = {"delete": {"ids": ids}}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"

        response = self.post_to_endpoint(url, payload)
        self.logger.debug(f"Rule Deletion Response: {json.dumps(response)}")

    def set_rules(self, rules: list[dict]) -> dict or None:
        """
        Adds given rules to the stream associated with the current BEARER_TOKEN
        """
        payload = {"add": rules}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"

        response = self.post_to_endpoint(url, payload)
        self.logger.debug(f"Rule Addition Respone: {json.dumps(response)}")

        try:
            if response["errors"]:
                self.logger.warning(
                    "setting rule returned an error: see log file for full response"
                )
                for num, error in enumerate(response["errors"]):
                    self.logger.warning(f"Error_{num} Title: {error['title']}")
                    self.logger.warning(f"Error_{num} Value: {error['value']}")
                    self.logger.warning(f"Error_{num} id: {error['id']}")

                return None
        except KeyError:
            info = {
                "rules": response["data"],
                "created": response["meta"]["summary"]["created"],
                "valid": response["meta"]["summary"]["valid"],
            }

            return info

    def stream(self):
        """
        Connects to the stream associated with the current BEARER_TOKEN
        """
        # response = requests.get(
        #     "https://api.twitter.com/2/tweets/search/stream", auth=self.bearer_oauth, stream=True,
        # )

        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream?expansions=author_id",
            auth=self.bearer_oauth,
            stream=True,
        )

        if response.status_code != 200:
            self.logger.error(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        self.logger.info("Starting Stream")
        for response_line in response.iter_lines():
            self.logger.info("Keep Alive Signal Received")
            if response_line:
                json_response = json.loads(response_line)
                self.logger.info(f"json respone: {json_response}")
                self.r.lpush("tweets", json.dumps(json_response))
        self.logger.error("STREAM BROKEN! ATTEMPTING TO TERMINATE!")


if __name__ == "__main__":
    bearer_token = os.environ.get("BEARER_TOKEN")
    handler = RedisHandler(bearer_token)
    handler.stream()
