# native
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http.client import responses
import json
import logging
import os
import pickle
import queue
from queue import Queue
import requests
import sqlite3
from threading import Event
import time
import warnings

# packages
import psycopg
import psycopg.sql as psql

# lib
from . import POSTGRES_ARGS, SQLLITE_ARGS


class TwitterHandler:
    """
    Python Object to control TwitterAPIv2 stream
    """

    def __init__(self, bearer_token: str, events:dict[str,Event], logger: logging.Logger):
        # To set your enviornment variables in your terminal run the following line:
        # export 'BEARER_TOKEN'='<your_bearer_token>'
        self.bearer_token = bearer_token
        self.logger = logger
        self.events = events

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
        self.logger.error("STREAM BROKEN! ATTEMPTING TO TERMINATE!")
        self.kill()
    

    def kill(self):
        self.logger.warning("Setting local_db flag")
        self.events["local_db"].set()
        self.logger.warning("local_db flag set")

        self.logger.warning("Setting sql flag")
        self.events["sql"].set()
        self.logger.warning("sql flag set")
        
        self.logger.warning("Setting killall flag")
        self.events["killall"].set()
        self.logger.warning("killall flag set")
        
                


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


class SQLlitePipe:
    def __init__(self, db_path, db_q, events: dict[str,Event], logger: logging.Logger):
        self.db_path = db_path
        self.db_q = db_q
        self.events = events
        self.logger = logger
        self.sleep_status = True

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
                        self.logger.info(f"Sleeping SQL for {timeout} seconds")
                        time.sleep(timeout)
                        retries += 1
                self.logger.info("Sleeping SQL DB")
                self.events["sql"].clear()
                self.wait_to_wake()

            return wrapper

        return the_real_decorator

    # ---

    def download_user_mapping(self):
        user_mapping = {}
        with sqlite3.connect(self.db_path) as conn:
            user_data = conn.execute("SELECT USER_ID,USER_NAME FROM ID_NAME_MAPPING;")
            for data in user_data:
                user_mapping[data[0]] = data[1]
        return user_mapping

    def get_sleep_status(self):
        return self.sleep_status

    def wait_to_wake(self):
        # while self.sleep_status:
        #     time.sleep(60)
        #     self.logger.info("Checking for Queue status")
        #     self.logger.info(f"Queue is empty: {self.db_q.empty()}")
        self.logger.info("Waiting...")
        self.events["sql"].wait()
        if self.events["killall"].is_set():
            self.logger.error("Kill command received while waiting to wake")
            self.logger.error("No longer attempting to connect to queue (SQLite)!")
        else:
            self.connect_to_queue()

    def execute_SQL(self, insert_values):
        self.logger.info("Executing SQL Commands")
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute(
                    """INSERT INTO TWEETS (TWEET_ID,AUTHOR_ID,AUTHOR_NAME,TWEET_TEXT) VALUES (?,?,?,?)""",
                    insert_values,
                )
                conn.commit()
            except sqlite3.Error as err:
                self.logger.error("Failure to add data")
                self.logger.error(err)
                conn.commit()
            self.logger.info("Change Commited")

    # @sleep_db(timeout=10)
    def connect_to_queue(self):
        self.logger.debug(f"SQL Thread Unlocked:{self.events['sql'].is_set()}")
        self.events["sql"].wait()
        self.logger.debug("Got to connect_to_queue function SQL")
        while self.db_q:
            try:
                sql_values = self.db_q.get(timeout=10)
                self.logger.debug(f"parsing values: {sql_values}")
                self.execute_SQL(sql_values)
            except queue.Empty:
                self.logger.info("Queue is empty, sleeping")
                self.logger.info("Sleeping SQL DB")
                self.events["sql"].clear()
                self.wait_to_wake()

                # raise queue.Empty


class PostgresPipe:
    def __init__(self, db_args, db_q, events: dict[str,Event], logger: logging.Logger):
        self.db_args = db_args
        self.connection = psycopg.connect(**self.db_args)
        self.db_q = db_q
        self.events = events
        self.logger = logger
        self.sleep_status = True

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
                        self.logger.info(f"Sleeping SQL for {timeout} seconds")
                        time.sleep(timeout)
                        retries += 1
                self.logger.info("Sleeping SQL DB")
                self.events["sql"].clear()
                self.wait_to_wake()

            return wrapper

        return the_real_decorator

    # ---

    def download_user_mapping(self):
        user_mapping = {}
        with self.connection as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    psql.SQL("SELECT user_id,user_name FROM {};").format(
                        psql.Identifier("id_name_mapping")
                    )
                )
            except Exception as err:
                self.logger.error(f"ERROR DOWNLOADING USER MAPPING {err}")
            try:
                user_data = cur.fetchall()
            except:
                user_data = None
            if user_data == None:
                self.logger.warning("id_name_mapping Empty. Is this expected?")
                return
            for data in user_data:
                user_mapping[data[0]] = data[1]
        self.logger.info("User Mapping Downloaded Successfully!")
        return user_mapping

    def get_sleep_status(self):
        return self.sleep_status

    def wait_to_wake(self):
        # while self.sleep_status:
        #     time.sleep(60)
        #     self.logger.info("Checking for Queue status")
        #     self.logger.info(f"Queue is empty: {self.db_q.empty()}")
        self.logger.info("Waiting...")
        self.events["sql"].wait()
        if self.events["killall"].is_set():
            self.logger.error("Kill command received while waiting to wake")
            self.logger.error("No longer attempting to connect to queue (Postgres)!")
        else:
            self.connect_to_queue()


    def execute_SQL(self, insert_values):
        self.logger.info("Executing SQL Commands")
        with self.connection as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    psql.SQL(
                        """INSERT INTO {} (tweet_id,author_id,author_name,tweet_text) VALUES (%s,%s,%s,%s)"""
                    ).format(psql.Identifier("tweets")),
                    insert_values,
                )
                conn.commit()
                self.logger.info("Change Commited")
            except psycopg.Error as err:
                self.logger.error(f"Failure to add data {err}")
                # conn.commit()

    # @sleep_db(timeout=10)
    def connect_to_queue(self):
        self.logger.debug(f"SQL Thread Unlocked:{self.events['sql'].is_set()}")
        self.events["sql"].wait()
        self.logger.info("Connecting to SQL Queue")
        while self.db_q:
            try:
                sql_values = self.db_q.get(timeout=10)
                self.logger.debug(f"parsing values: {sql_values}")
                self.execute_SQL(sql_values)
            except queue.Empty:
                self.logger.info("Queue is empty, sleeping")
                self.logger.info("Sleeping SQL DB")
                self.events["sql"].clear()
                self.wait_to_wake()

                # raise queue.Empty


class TweetDB:
    """
    Maintains all the tweets that are coming in from the stream.
    Allows for text processing to be moved to a different thread to reduce load on Stream thread
    Maps tweet_ids to Tweet objects
    """

    def __init__(
        self,
        tweet_dict: dict,
        response_q: Queue,
        db_q: Queue,
        events: dict[str,Event],
        id_mapping: dict,
        logger: logging.Logger,
    ):
        self.tweet_dict = tweet_dict
        self.response_q = response_q
        self.events = events
        self.id_mapping = id_mapping
        self.db_q = db_q
        self.logger = logger

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
                        self.logger.info(f"Sleeping DB for {timeout} seconds")
                        time.sleep(timeout)
                        retries += 1
                # self.offload_db()
                self.logger.info("Sleeping DB")
                self.events["local_db"].clear()
                self.wait_to_wake()

            return wrapper

        return the_real_decorator

    # ---

    def wait_to_wake(self):
        # while self.sleep_status:
        #     time.sleep(60)
        #     self.logger.info("Checking for Queue status")
        #     self.logger.info(f"Queue is empty: {self.response_q.empty()}")
        self.logger.info("Waiting...")
        self.events["local_db"].wait()
        if self.events["killall"].is_set():
            self.logger.error("Kill command received while waiting to wake")
            self.logger.error("No longer attempting to connect to queue (local_db)!")
        else:
            self.connect_to_queue()

    def get_sleep_status(self):
        return self.sleep_status

    def cache(self, json_response: dict) -> None:
        self.logger.info(f"Adding to Cache {json_response}")
        self.response_q.put(json_response)
        self.events["local_db"].set()
        self.logger.info(f"Local DB awoken {self.events['local_db'].is_set()}!")

    def parse(self, tweet_data: dict) -> None:
        tweet_id = tweet_data["data"]["id"]
        self.logger.info(f"Parsing Tweet {tweet_id}")
        tweet_text = tweet_data["data"]["text"].replace("\n", "")
        self.logger.debug(f"Tweet Text: {tweet_text}")
        tweet_author = tweet_data["data"]["author_id"]
        self.logger.debug(f"Tweet Author: {tweet_author}")
        self.tweet_dict[tweet_id] = Tweet(tweet_id, tweet_text, tweet_author)
        # tweet_author = get_author(tweet_id) # ! add an error catch for this !
        # self.tweet_dict[tweet_id].set_author_id(tweet_author)
        try:
            self.db_q.put(
                (
                    int(tweet_id),
                    int(tweet_author),
                    self.id_mapping[int(tweet_author)],
                    str(tweet_text),
                )
            )
        except KeyError:
            self.logger.warning(f"Mapping unavailable for {tweet_author}")

        except:
            self.logger.error("UNKNOWN EXCEPTION")

        self.logger.info("Tweet Parsed, Adding to DB Q")

    # @sleep_db(timeout=1)
    def connect_to_queue(self):
        self.logger.debug(f"Local DB Unlocked: {self.events['local_db'].is_set()}")
        self.events["local_db"].wait()
        self.logger.debug("Got to connect_to_queue function DB")
        while self.response_q:
            try:
                json_obj = self.response_q.get(timeout=10)
                self.logger.debug(f"parsing obj: {json_obj}")
                # self.parse(json_obj, self.get_author)
                self.parse(json_obj)
                self.logger.debug(f"SQL DB Unlocked {self.events['sql'].is_set()}")
                if not self.events["sql"].is_set():
                    self.logger.debug(f"Unlocking SQL DB Thread")
                    self.events["sql"].set()
            except queue.Empty:
                self.logger.info("Queue is empty, sleeping")
                # self.offload_db()
                self.logger.info("Sleeping DB")
                self.events["local_db"].clear()
                self.wait_to_wake()

                # raise queue.Empty

    def offload_db(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        fname = "_data/pickles/" + timestr + "_tweetDB.pickle"
        with open(fname, "wb") as handle:
            self.logger.info("Dumping current dict to pickle")
            pickle.dump(self.tweet_dict, handle)


class TweetStream:
    def __init__(self, bearer_token: str, db_path: str):
        self.log_root = self.create_loggers()

        # self.log_root.info(self.user_mapping)
        self.tweet_dict = {}
        self.tweet_q = Queue(0)
        self.db_q = Queue(0)

        self.events = {"local_db": Event(), "sql": Event(), "killall":Event()}
        # self.events['local_db'].set()
        # self.events['sql'].set()
        self.handler = TwitterHandler(bearer_token, logging.getLogger("Handler"))
        # self.handler = fakeTwitterHandler(logging.getLogger("Handler"))
        # self.sql_pipe = SQLlitePipe(
        #     db_path, self.db_q, self.events, logging.getLogger("SQL_Database")
        # )
        self.sql_pipe = PostgresPipe(
            db_path, self.db_q, self.events, logging.getLogger("SQL_Database")
        )
        self.user_mapping = self.sql_pipe.download_user_mapping()
        self.database = TweetDB(
            self.tweet_dict,
            self.tweet_q,
            self.db_q,
            self.events,
            self.user_mapping,
            logging.getLogger("Local_Dict"),
        )

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

        # logging.basicConfig(level=logging.DEBUG)

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
        # fh_root = logging.FileHandler("logs/ROOT_LOG.log", mode="w+")
        # fh_root.setFormatter(formatter)

        log_handler.addHandler(fh_handler)
        log_handler.addHandler(ch)

        log_dict.addHandler(fh_dict)
        log_dict.addHandler(ch)

        log_sql.addHandler(fh_sql)
        log_sql.addHandler(ch)

        # log_root.addHandler(fh_root)
        # log_root.addHandler(ch)

        return log_root

    def cache(self):
        for json_response in self.handler.stream():
            self.database.cache(json_response)
            if not self.events["local_db"].is_set():
                self.log_root.debug("Waking Local DB")
                self.events["local_db"].set()

    def parse(self):
        self.database.connect_to_queue()

    def offload(self):
        self.sql_pipe.connect_to_queue()

    def run(self):
        with ThreadPoolExecutor(4) as executor:
            cache_future = executor.submit(self.cache)
            parse_future = executor.submit(self.parse)
            offload_future = executor.submit(self.offload)
            # self.log_root(threading.excepthook(cache_future))
            # if cache_future:
            #     self.log_root(cache_future)
            # if parse_future:
            #     self.log_root(parse_future)
            # if offload_future:
            #     self.log_root(offload_future)


# ! Add Author DB, maps author to tweet items

if __name__ == "__main__":
    bearer_token = os.environ.get("BEARER_TOKEN")
    # stream = TweetStream(bearer_token, "test.db")
    # postgres_args = {"host": "localhost", "dbname": "template1", "user": "postgres"}
    stream = TweetStream(bearer_token, POSTGRES_ARGS)
    stream.run()
