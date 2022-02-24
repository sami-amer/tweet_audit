"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/
""" 
import requests
import os
import json
import logging
import pandas as pd

## Logging Setup
logging.basicConfig(filename='AUDIT_LOG.log', level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(ch)



class TwitterStream:
    """
    Python Object to control TwitterAPIv2 stream
    """

    def __init__(self): # ? Should this init with the bearer token?
        # To set your enviornment variables in your terminal run the following line:
        # export 'BEARER_TOKEN'='<your_bearer_token>'
        self.bearer_token = os.environ.get("BEARER_TOKEN")

    def bearer_oauth(self,r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserLookupPython"
        return r
    
    def connect_to_endpoint(self,url:str,params=None) -> json:
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
        data = self.connect_to_endpoint(url)['data'][0]

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
        data = self.connect_to_endpoint(url, params)['data'] # ? Make this a default dict?
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
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        logging.debug(f"Rule Get Response: {json.dumps(response.json())}")
        try:
            data = response.json()["data"]
            meta = response.json()["meta"]
            info = {
            "rules": data,
            "rule_count":meta["result_count"]
        }
            logging.info(info)
            return info, response.json()
        except KeyError:
            logging.warning("No Rules Found!")
            return None, response.json()
    
    def delete_all_rules(self,rules_response:json) -> None:
        """
        Deletes all the rules on the stream associated with the current BEARER_TOKEN
        """
        if rules_response is None or "data" not in rules_response:
            return None

        ids = list(map(lambda rule: rule["id"], rules_response["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        logging.debug(f"Rule Deletion Response: {json.dumps(response.json())}")

    def delete_rules(self,ids:list) -> None:
        """
        Deletes specific rules on the stream associated with the current BEARER_TOKEN

        Arguments:
            ids (list):
        """
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        logging.debug(f"Rule Deletion Response: {json.dumps(response.json())}")

    def set_rules(self,rules: list[dict]) -> dict or None:
        """
        Adds given rules to the stream associated with the current BEARER_TOKEN 
        """
        payload = {"add":rules}
        print(payload)
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        
        logging.debug(f"Rule Addition Respone: {json.dumps(response.json())}")

        try:
            if response.json()["errors"]:
                logging.warning("setting rule returned an error: see log file for full response")
                for num,error in enumerate(response.json()["errors"]):
                    logging.warning(f"Error_{num} Title: {error['title']}")
                    logging.warning(f"Error_{num} Value: {error['value']}")
                    logging.warning(f"Error_{num} id: {error['id']}")
                
                return None
        except KeyError:
            info = {
                "rules":response.json()["data"],
                "created":response.json()["meta"]["summary"]["created"],
                "valid":response.json()["meta"]["summary"]["valid"],
            }

            return info

    def connect(self):
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
        # ! is container the best way to do this?
        with open("texts.txt","a") as f: # ! make this stream into a python object as well
            for response_line in response.iter_lines():
                if response_line:
                    json_response = json.loads(response_line)
                    f.write(f"\nID FOR FOLLOWING TWEET: {json_response['data']['id']}\n")
                    f.write(json_response["data"]["text"])
                    logging.info(json.dumps(json_response, indent=4, sort_keys=True))
                    # f.write(json_response)

    def add_users(self,user_ids:list[tuple]) -> None:
        rules = []
        for id,tag in user_ids:
            rules.append({"value": f"from:{id}", "tag": f"{tag}"})
        
        for rule in rules:
            logging.info(f"Adding Rule: {rule}")
        self.set_rules(rules)

    # ! MOVE THIS SOMEWHERE ELSE, IMPLEMENT IT TO ADD_Users
    def format_rules(self,usernames): # ! Use Usernames over User IDs because they are capped at 15
        rules = []
        for i in range(0,len(usernames),22): # ! if this can find average, can be more efficient
            # ! Add check for abnormal lengths
            rule = ''
            for user in usernames[i:i+22]:
                rule+=f"from:{user} OR "
            rules.append(rule[:-4:])
        return rules

if __name__ == '__main__':
    stream = TwitterStream()
    # ids = [stream.get_user_id("Sami_Amer_PS")["id"], stream.get_user_id("h_jackson_")["id"]]

    # df = pd.read_csv("_data/senate_usernames_dec21.csv")
    # usernames = list(df["username"])
    # # print(usernames)
    # # stream.add_users(usernames)
    # a = stream.format_rules(usernames)
    # # _,response = stream.get_rules()
    # # stream.delete_all_rules(response)
    # for rule in a:
    #     # print({"value":rule})
    #     stream.set_rules([{"value":rule}])
    stream.connect()
    # stream.get_rules()
    # TODO: create tools.py to import from
    # TODO:     make the format_users a tool
    # TODO:     Make a tool to take UCSD xlsx to a csv to use, and then to a list
    # TODO: unit tests
    # TODO: make it so rules can be updated by less than max length
    # TODO:     set the "tag" to be rule length
    # TODO:     Find shortest rule, add to it as much as possible
    # TODO:     keep going until all people added
    # TODO: make format_user account for avg length of list or other metric that can maximize rule size
    # TODO: flush response faster to txt file
    # TODO: get username from tweet id
    # TODO: decide what kind of DB to store tweets in. list of JSONs?
    # TODO: Match accounts to official congressional sites/accounts
