from dataclasses import dataclass
from pyclbr import Function
from queue import Queue
import queue, requests, os, json, logging, time, pickle, sqlite3
import pandas as pd
from glob import glob
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

    def bearer_oauth(self,r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserLookupPython"
        return r
    
    def get_from_endpoint(self,url:str,params=None) -> dict:
        """
        Connnects to the Twitter API using the given url and params

        Arguments:
            url     (str): the url genereated by a function 
            params  (str): parameters that need to be passed with the url [optional]
        """
        response = requests.request("GET", url, auth=self.bearer_oauth,params=params)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()

    def post_to_endpoint(self,url:str, payload:dict) -> dict:
        response = requests.post(
            url,
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        return response

    def get_user_id(self,user:str) -> dict:
        """
        Given a twitter username without "@", returns the user id

        Arguments:
            user    (str): the twitter username without '@'
        """
        # Specify the usernames that you want to lookup below
        # You can enter up to 100 comma-separated values.
        usernames = f"usernames={user}"
        user_fields = "user.fields=id,verified,description,created_at"
        # User fields are adjustable, options include:
        # created_at, description, entities, id, location, name,
        # pinned_tweet_id, profile_image_url, protected,
        # public_metrics, url, username, verified, and withheld
        url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
        data = self.get_from_endpoint(url)['data'][0]

        info = {
            "id": data['id'],
            "name": data['name'],
            "username": data["username"],
            "description": data['description'],
            "verified": data['verified'],
            "creation_date": data['created_at']
        }
        return info

    def get_user_timeline(self,user_id:str,max_results=5) -> list[dict]:
        """
        Given a user id, return some tweets from the users timeline

        Arguments:
            user_id (str): the users id
        """
        url= "https://api.twitter.com/2/users/{}/tweets".format(user_id)
        params = {"tweet.fields": "text,source,author_id,attachments","max_results":str(max_results)}
        data = self.get_from_endpoint(url, params)['data'] # ? Make this a default dict?
        tweets = []
        for tweet in data:
            info = {
                "id" : tweet["id"],
                "text": tweet["text"],
                "author_id": tweet["author_id"], # ! Add check for this
                "source": tweet["source"],
                #"attachments": tweet["attachments"] if tweet["attachments"] else None ! add check for this
            }
            tweets.append(info)
        return tweets

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
            info = {
            "rules": data,
            "rule_count":meta["result_count"]
        }
            self.logger.info(info)
            return info, response
        except KeyError:
            self.logger.warning("No Rules Found!")
            return None, response
    
    def delete_all_rules(self,rules_response:json) -> None:
        """
        Deletes all the rules on the stream associated with the current BEARER_TOKEN
        """
        if rules_response is None or "data" not in rules_response:
            return None

        ids = list(map(lambda rule: rule["id"], rules_response["data"]))
        payload = {"delete": {"ids": ids}}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"
        response = self.post_to_endpoint(url,payload)

        self.logger.debug(f"Rule Deletion Response: {json.dumps(response)}")

    def delete_rules(self,ids:list) -> None:
        """
        Deletes specific rules on the stream associated with the current BEARER_TOKEN

        Arguments:
            ids (list):
        """
        payload = {"delete": {"ids": ids}}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"

        response = self.post_to_endpoint(url, payload) 
        self.logger.debug(f"Rule Deletion Response: {json.dumps(response)}")

    def set_rules(self,rules: list[dict]) -> dict or None:
        """
        Adds given rules to the stream associated with the current BEARER_TOKEN 
        """
        payload = {"add":rules}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"
        
        response = self.post_to_endpoint(url, payload)
        self.logger.debug(f"Rule Addition Respone: {json.dumps(response)}")

        try:
            if response["errors"]:
                self.logger.warning("setting rule returned an error: see log file for full response")
                for num,error in enumerate(response["errors"]):
                    self.logger.warning(f"Error_{num} Title: {error['title']}")
                    self.logger.warning(f"Error_{num} Value: {error['value']}")
                    self.logger.warning(f"Error_{num} id: {error['id']}")
                
                return None
        except KeyError:
            info = {
                "rules":response["data"],
                "created":response["meta"]["summary"]["created"],
                "valid":response["meta"]["summary"]["valid"],
            }

            return info

    def stream(self):
        """
        Connects to the stream associated with the current BEARER_TOKEN
        """
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream", auth=self.bearer_oauth, stream=True,
        )

        if response.status_code != 200:
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
                

    def add_users(self,user_ids:list[tuple]) -> None:
        rules = []
        for id,tag in user_ids:
            rules.append({"value": f"from:{id}", "tag": f"{tag}"})
        
        for rule in rules:
            self.logger.info(f"Adding Rule: {rule}")
        self.set_rules(rules)


    def get_user_from_tweet(self,id: str):
        tweet_fields = "tweet.fields=lang,author_id"
        # ids = "ids=1278747501642657792,1255542774432063488"
        id = f"ids={id}"
        url = "https://api.twitter.com/2/tweets?{}&{}".format(id, tweet_fields) # ? Maybe use [text] response to double checK?

        data = self.get_from_endpoint(url)
        user_id = data["data"][0]["author_id"]

        # print(json.dumps(data, indent=4, sort_keys=True))
        self.logger.info(f"User ID Query Returned: {user_id}")
        return user_id
@dataclass
class Tweet:
    """
    Represents each tweet after it gets parsed by TweetDB
    """

    tweet_id: str # better than int due to id lengths
    tweet_text: str
    author_id: str = None
    logger: logging.Logger = logging.getLogger()

    def set_author_id(self, author_id:str) -> None:
        old_author_id = self.author_id
        self.author_id = author_id
        self.logger.debug(f"Updated author_id for tweet: {self.tweet_id} from {old_author_id} to {self.author_id}")


@dataclass
class TweetDB:
    """
    Maintains all the tweets that are coming in from the stream. 
    Allows for text processing to be moved to a different thread to reduce load on Stream thread
    Maps tweet_ids to Tweet objects
    """

    tweet_dict: dict
    q: Queue # ! make a concrete size as we learn more
    get_author: Function
    logger: logging.Logger
    sleep_status: bool = False

    # Add methods to add to queue, remove from queue, parse, and add to tweet_dict
    # --- adapted from realpython.org
    # --- https://realpython.com/python-sleep/
    def sleep_db(timeout, retry=3):
        def the_real_decorator(function):
            def wrapper(self,*args, **kwargs):
                retries = 0
                while retries < retry:
                    try:
                        value = function(self,*args, **kwargs)
                        if value is None:
                            return
                    except:
                        self.logger.info(f'Sleeping for {timeout} seconds')
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
            self.logger.info(f"Queue is empty: {self.q.empty()}")
        self.connect_to_queue()


    def get_sleep_status(self):
        return self.sleep_status

    def cache(self,json_response: dict) -> None:
        self.logger.info(f"Adding to Cache {json_response}")
        self.q.put(json_response)

    def parse(self,tweet_data: dict, get_author: Function) -> None:
        tweet_id = tweet_data["data"]["id"]
        self.logger.info(f"Parsing Tweet {tweet_id}")
        tweet_text = tweet_data["data"]["text"]
        self.tweet_dict[tweet_id] = Tweet(tweet_id, tweet_text)
        tweet_author = get_author(tweet_id) # ! add an error catch for this !
        self.tweet_dict[tweet_id].set_author_id(tweet_author)
        # self.logger.debug(tweet_author)
        # self.logger.debug("_"*75)
        # self.logger.debug("Current Tweet Dict is:")
        # self.logger.debug(self.tweet_dict)
        # self.logger.debug("_"*75)
    
    @sleep_db(timeout=60)
    def connect_to_queue(self):
        self.logger.debug("Got to connect_to_queue function")
        while self.q:
            try:
                json_obj = self.q.get(timeout=5)
                self.logger.debug(f"parsing obj: {json_obj}")
                self.parse(json_obj, self.get_author)
            except queue.Empty:
                self.logger.info("Queue is empty")
                raise queue.Empty

    def offload_db(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        fname = "_data/pickles/"+timestr + "_tweetDB.pickle"
        with open(fname, 'wb') as handle:
            self.logger.info("Dumping current dict to pickle")
            pickle.dump(self.tweet_dict, handle)
@dataclass
class TweetDB_SQL(TweetDB):
    
    db: sqlite3.Connection = sqlite3.connect("test.db")

    def parse(self, tweet_data: dict, get_author: Function) -> None:
        tweet_id = tweet_data["data"]["id"]
        self.logger.info(f"Parsing Tweet {tweet_id}")
        tweet_text = tweet_data["data"]["text"]
        self.tweet_dict[tweet_id] = Tweet(tweet_id, tweet_text)
        tweet_author = get_author(tweet_id) # ! add an error catch for this !
        self.tweet_dict[tweet_id].set_author_id(tweet_author)
        self.db.execute("INSERT INTO TWEETS VALUES (?,?,?,?)",(tweet_id,tweet_author,"NA",tweet_text))
        self.db.commit()
    
class TweetStream:
    
    def __init__(self,bearer_token:str):
        self.create_loggers()
        self.tweet_dict = {}
        self.tweet_q = Queue(0)
        self.handler = TwitterHandler(bearer_token,logging.getLogger('Handler'))
        # self.database = TweetDB(self.tweet_dict,self.tweet_q,self.handler.get_user_from_tweet,logging.getLogger('Database'))
        self.database = TweetDB_SQL(self.tweet_dict,self.tweet_q,self.handler.get_user_from_tweet,logging.getLogger('Database'))
    
    @staticmethod
    def create_loggers() -> None:
        formatter = logging.Formatter('%(asctime)s [%(name)s][%(levelname)s] %(message)s')
        logging.basicConfig(level=logging.DEBUG, filename="logs/ROOT_LOG.log", format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')
        log_handler = logging.getLogger('Handler')
        log_db = logging.getLogger('Database')
        log_tweet = logging.getLogger('Tweet')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        fh_handler = logging.FileHandler("logs/HANDLER_lOG.log")
        fh_handler.setFormatter(formatter)
        fh_db = logging.FileHandler("logs/DB_LOG.log")
        fh_db.setFormatter(formatter)
        fh_tweet = logging.FileHandler("logs/TWEET_LOG.log")
        fh_tweet.setFormatter(formatter)

        log_handler.addHandler(fh_handler)
        log_handler.addHandler(ch)

        log_db.addHandler(fh_db)
        log_db.addHandler(ch)

        log_tweet.addHandler(fh_tweet)
        log_tweet.addHandler(ch)

    def cache(self):
        for json_response in self.handler.stream():
            # self.tweet_q.put(json_response)
            self.database.cache(json_response)
            if self.database.get_sleep_status():
                self.database.sleep_status = False

    def parse(self):
        self.database.connect_to_queue()
    
    def run(self):
        with ThreadPoolExecutor(4) as executor:
            executor.submit(self.cache)
            executor.submit(self.parse)

# ! Add Author DB, maps author to tweet items
# ! Add Local ID:NAME DB

if __name__ == '__main__':
    bearer_token = os.environ.get("BEARER_TOKEN")
    stream = TweetStream(bearer_token)
    stream.run()