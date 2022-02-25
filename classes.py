from dataclasses import dataclass
import requests
import os
import json
import logging
import pandas as pd
import tools

class TwitterStream:
    """
    Python Object to control TwitterAPIv2 stream
    """

    def __init__(self, bearer_token):
        # To set your enviornment variables in your terminal run the following line:
        # export 'BEARER_TOKEN'='<your_bearer_token>'
        self.bearer_token = bearer_token
        self.tweet_db = {}

    def bearer_oauth(self,r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserLookupPython"
        return r
    
    def get_from_endpoint(self,url:str,params=None) -> json:
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

    def post_to_endpoint(self,url:str, payload:json) -> json:
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
        logging.debug(f"Rule Get Response: {json.dumps(response)}")
        try:
            data = response["data"]
            meta = response["meta"]
            info = {
            "rules": data,
            "rule_count":meta["result_count"]
        }
            logging.info(info)
            return info, response
        except KeyError:
            logging.warning("No Rules Found!")
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

        logging.debug(f"Rule Deletion Response: {json.dumps(response)}")

    def delete_rules(self,ids:list) -> None:
        """
        Deletes specific rules on the stream associated with the current BEARER_TOKEN

        Arguments:
            ids (list):
        """
        payload = {"delete": {"ids": ids}}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"

        response = self.post_to_endpoint(url, payload) 
        logging.debug(f"Rule Deletion Response: {json.dumps(response)}")

    def set_rules(self,rules: list[dict]) -> dict or None:
        """
        Adds given rules to the stream associated with the current BEARER_TOKEN 
        """
        payload = {"add":rules}
        url = "https://api.twitter.com/2/tweets/search/stream/rules"
        
        response = self.post_to_endpoint(url, payload)
        logging.debug(f"Rule Addition Respone: {json.dumps(response)}")

        try:
            if response["errors"]:
                logging.warning("setting rule returned an error: see log file for full response")
                for num,error in enumerate(response["errors"]):
                    logging.warning(f"Error_{num} Title: {error['title']}")
                    logging.warning(f"Error_{num} Value: {error['value']}")
                    logging.warning(f"Error_{num} id: {error['id']}")
                
                return None
        except KeyError:
            info = {
                "rules":response["data"],
                "created":response["meta"]["summary"]["created"],
                "valid":response["meta"]["summary"]["valid"],
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
        # ! look into streaming to a python object, then reading that stream on another thread
        with open("texts.txt","a") as f: # ! make this stream into a python object as well
            for response_line in response.iter_lines():
                if response_line:
                    json_response = json.loads(response_line)
                    to_write = f"ID FOR FOLLOWING TWEET: {json_response['data']['id']}\n" + json_response["data"]["text"] + "\n" + "_"*100
                    # ? the things we print out will change over time
                    f.write(to_write)
                    logging.info(to_write)


    def add_users(self,user_ids:list[tuple]) -> None:
        rules = []
        for id,tag in user_ids:
            rules.append({"value": f"from:{id}", "tag": f"{tag}"})
        
        for rule in rules:
            logging.info(f"Adding Rule: {rule}")
        self.set_rules(rules)


    def get_user_from_tweet(self,id: str):
        tweet_fields = "tweet.fields=lang,author_id"
        # ids = "ids=1278747501642657792,1255542774432063488"
        id = f"ids={id}"
        url = "https://api.twitter.com/2/tweets?{}&{}".format(id, tweet_fields) # ? Maybe use [text] response to double checK?

        data = self.get_from_endpoint(url)
        user_id = data["data"][0]["author_id"]

        # print(json.dumps(data, indent=4, sort_keys=True))
        logging.info(f"User ID Query Returned: {user_id}")
        return user_id

