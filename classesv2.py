from dataclasses import dataclass
from pyclbr import Function
from queue import Queue
import queue, requests, os, json, logging, time, pickle, sqlite3, warnings
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


class TwitterHandler:
    """
    Python Object to control TwitterAPIv2 stream
    """

    def __init__(self, bearer_token: str, logger: logging.Logger):
        # To set your enviornment variables in your terminal run the following line:
        # export 'BEARER_TOKEN'='<your_bearer_token>'
        self.bearer_token = bearer_token
        self.logger = logger

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

        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                self.logger.debug(f"json respone: {json_response}")
                yield json_response
        self.logger.error("STREAM BROKEN!")

    # def get_user_from_tweet(self,id: str):
    #     tweet_fields = "tweet.fields=lang,author_id"
    #     # ids = "ids=1278747501642657792,1255542774432063488"
    #     id = f"ids={id}"
    #     url = "https://api.twitter.com/2/tweets?{}&{}".format(id, tweet_fields) # ? Maybe use [text] response to double checK?

    #     data = self.handler.get_from_endpoint(url)
    #     user_id = data["data"][0]["author_id"]

    #     # print(json.dumps(data, indent=4, sort_keys=True))
    #     self.logger.info(f"User ID Query Returned: {user_id}")
    #     return user_id


@dataclass
class Tweet:
    """
    Represents each tweet after it gets parsed by TweetDB
    """

    tweet_id: str  # better than int due to id lengths
    tweet_text: str
    author_id: str = None
    logger: logging.Logger = logging.getLogger()

    def set_author_id(self, author_id: str) -> None:
        old_author_id = self.author_id
        self.author_id = author_id
        self.logger.debug(
            f"Updated author_id for tweet: {self.tweet_id} from {old_author_id} to {self.author_id}"
        )

    def __str__(self):
        return (self.tweet_id, self.author_id, self.tweet_text)

    def get_dict(self):
        return {
            "tweet_id": self.tweet_id,
            "author_id": self.author_id,
            "tweet_text": self.tweet_text,
        }


@dataclass
class TweetDB:
    """
    Maintains all the tweets that are coming in from the stream.
    Allows for text processing to be moved to a different thread to reduce load on Stream thread
    Maps tweet_ids to Tweet objects
    """

    tweet_dict: dict
    response_q: Queue  # ! make a concrete size as we learn more
    db_q: Queue
    mapping: dict
    logger: logging.Logger
    sleep_status: bool = False

    # --- adapted from realpython.org
    # --- https://realpython.com/python-sleep/
    def sleep_db(timeout, retry=3):
        def the_real_decorator(function):
            def wrapper(self, *args, **kwargs):
                retries = 0
                while retries < retry:
                    try:
                        value = function(self, *args, **kwargs)
                        if value is None:
                            return
                    except:
                        self.logger.info(f"Sleeping for {timeout} seconds")
                        time.sleep(timeout)
                        retries += 1
                self.offload_db()
                self.logger.info("Sleeping DB")
                self.sleep_status = True
                self.wait_to_wake()

            return wrapper

        return the_real_decorator

    # ---

    def wait_to_wake(self):
        while self.sleep_status:
            time.sleep(60)
            self.logger.info("Checking for Queue status")
            self.logger.info(f"Queue is empty: {self.response_q.empty()}")
        self.connect_to_queue()

    def get_sleep_status(self):
        return self.sleep_status

    def cache(self, json_response: dict) -> None:
        self.logger.info(f"Adding to Cache {json_response}")
        self.response_q.put(json_response)

    def parse(self, tweet_data: dict) -> None:
        tweet_id = tweet_data["data"]["id"]
        self.logger.info(f"Parsing Tweet {tweet_id}")
        tweet_text = tweet_data["data"]["text"]
        self.logger.debug(f"Tweet Text: {tweet_text}")
        tweet_author = tweet_data["data"]["author_id"]
        self.logger.debug(f"Tweet Author: {tweet_author}")
        self.tweet_dict[tweet_id] = Tweet(tweet_id, tweet_text,tweet_author)
        # tweet_author = get_author(tweet_id) # ! add an error catch for this !
        # self.tweet_dict[tweet_id].set_author_id(tweet_author)
        self.logger.info("Tweet Parsed, Adding to DB Q")
        self.db_q.put(
            (
                int(tweet_id),
                int(tweet_author),
                self.mapping[int(tweet_author)],
                str(tweet_text),
            )
        )

    @sleep_db(timeout=60)
    def connect_to_queue(self):
        self.logger.debug("Got to connect_to_queue function")
        while self.response_q:
            try:
                json_obj = self.response_q.get(timeout=5)
                self.logger.debug(f"parsing obj: {json_obj}")
                # self.parse(json_obj, self.get_author)
                self.parse(json_obj)
            except queue.Empty:
                self.logger.info("Queue is empty")
                raise queue.Empty

    def offload_db(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        fname = "_data/pickles/" + timestr + "_tweetDB.pickle"
        with open(fname, "wb") as handle:
            self.logger.info("Dumping current dict to pickle")
            pickle.dump(self.tweet_dict, handle)


class SQLPipe:
    def __init__(self, db_path, db_q, logger: logging.Logger):
        self.db_path = db_path
        self.db_q = db_q
        self.logger = logger
        self.sleep_status = False

    # --- adapted from realpython.org
    # --- https://realpython.com/python-sleep/
    def sleep_db(timeout, retry=3):
        def the_real_decorator(function):
            def wrapper(self, *args, **kwargs):
                retries = 0
                while retries < retry:
                    try:
                        value = function(self, *args, **kwargs)
                        if value is None:
                            return
                    except:
                        self.logger.info(f"Sleeping for {timeout} seconds")
                        time.sleep(timeout)
                        retries += 1
                self.logger.info("Sleeping SQL DB")
                self.sleep_status = True
                self.wait_to_wake()
                self.db.commit()

            return wrapper

        return the_real_decorator

    # ---

    def get_sleep_status(self):
        return self.sleep_status

    def wait_to_wake(self):
        while self.sleep_status:
            time.sleep(60)
            self.logger.info("Checking for Queue status")
            self.logger.info(f"Queue is empty: {self.db_q.empty()}")
        self.connect_to_queue()

    def execute_SQL(self, insert_values):
        self.logger.info("Executing SQL Commands")
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute(
                    """INSERT INTO TWEETS (TWEET_ID,AUTHOR_ID,AUTHOR_NAME,TWEET_TEXT) VALUES (?,?,?,?)""",
                    insert_values,
                )
            except sqlite3.Error as err:
                self.logger.error("Failure to add data")
                self.logger.error(err)
            conn.commit()
            self.logger.info("Change Commited")

    @sleep_db(timeout=60)
    def connect_to_queue(self):
        self.logger.debug("Got to connect_to_queue function")
        while self.db_q:
            try:
                sql_values = self.db_q.get(timeout=5)
                self.logger.debug(f"parsing values: {sql_values}")
                self.execute_SQL(sql_values)
            except queue.Empty:
                self.logger.info("Queue is empty")
                raise queue.Empty


class TweetStream:
    def __init__(self, bearer_token: str, db_path: str):
        log_root = self.create_loggers()
        self.user_mapping = {}
        with sqlite3.connect(db_path) as conn:
            user_data = conn.execute("SELECT USER_ID,USER_NAME FROM ID_NAME_MAPPING;")
            for data in user_data:
                self.user_mapping[data[0]] = data[1]
        log_root.info(self.user_mapping)
        self.tweet_dict = {}
        self.tweet_q = Queue(0)
        self.db_q = Queue(0)
        self.handler = TwitterHandler(bearer_token, logging.getLogger("Handler"))
        self.database = TweetDB(
            self.tweet_dict,
            self.tweet_q,
            self.db_q,
            self.user_mapping,
            logging.getLogger("Local_Dict"),
        )
        self.SQL_PIPE = SQLPipe(
            db_path, self.db_q, logging.getLogger("SQL_Database")
        )  # ! Make this modular

    @staticmethod
    def create_loggers() -> logging.Logger:
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s][%(levelname)s] %(message)s"
        )
        logging.basicConfig(
            level=logging.DEBUG,
            filename="logs/ROOT_LOG.log",
            format="%(asctime)s [%(name)s][%(levelname)s] %(message)s",
        )
        log_root = logging.getLogger()
        log_handler = logging.getLogger("Handler")
        log_dict = logging.getLogger("Local_Dict")
        log_sql = logging.getLogger("SQL_Database")

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        fh_handler = logging.FileHandler("logs/HANDLER_LOG.log", mode="w+")
        fh_handler.setFormatter(formatter)
        fh_dict = logging.FileHandler("logs/DB_LOG.log", mode="w+")
        fh_dict.setFormatter(formatter)
        fh_sql = logging.FileHandler("logs/SQL_LOG.log", mode="w+")
        fh_sql.setFormatter(formatter)

        log_handler.addHandler(fh_handler)
        log_handler.addHandler(ch)

        log_dict.addHandler(fh_dict)
        log_dict.addHandler(ch)

        log_sql.addHandler(fh_sql)
        log_sql.addHandler(ch)

        return log_root

    def cache(self):
        for json_response in self.handler.stream():
            self.database.cache(json_response)
            if self.database.get_sleep_status():
                self.database.sleep_status = False
            if self.SQL_PIPE.get_sleep_status():
                self.SQL_PIPE.sleep_status = False

    def parse(self):
        self.database.connect_to_queue()

    def offload(self):
        self.SQL_PIPE.connect_to_queue()

    def run(self):
        with ThreadPoolExecutor(4) as executor:
            executor.submit(self.cache)
            executor.submit(self.parse)
            executor.submit(self.offload)


# ! Add Author DB, maps author to tweet items

if __name__ == "__main__":
    bearer_token = os.environ.get("BEARER_TOKEN")
    stream = TweetStream(bearer_token, "test.db")
    stream.run()
