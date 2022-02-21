"""
Full pipeline control for tweet audit
Adapted from the official TwitterAPIv2 Sample Code
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/
""" 
import requests
import os
import json
import logging

## Logging Setup
logging.basicConfig(filename='AUDIT_LOG.log', level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger()
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(ch)

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r

def connect_to_endpoint(url,params=None):
    """
    Connnects to the Twitter API using the given url and params

    Arguments:
        url     (str): the url genereated by a function 
        params  (str): parameters that need to be passed with the url [optional]
    """
    response = requests.request("GET", url, auth=bearer_oauth,params=params)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def get_user_id(user):
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
    data = connect_to_endpoint(url)['data'][0]

    info = {
        "id": data['id'],
        "name": data['name'],
        "username": data["username"],
        "description": data['description'],
        "verified": data['verified'],
        "creation_date": data['created_at']
    }
    return info

def get_user_timeline(user_id): # ! is this variable in size?
    """
    Given a user id, return some tweets from the users timeline

    Arguments:
        user_id (str): the users id
    """
    url= "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    params = {"tweet.fields": "text,source,author_id,attachments"}
    data = connect_to_endpoint(url, params)['data'][0] # ? Make this a default dict?
    info = {
        "id" : data["id"],
        "text": data["text"],
        "author_id": data["author_id"], # ! Add check for this
        "source": data["source"],
        #"attachments": data["attachments"] if data["attachments"] else None ! add check for this
    }
    return info

def get_stream_rules(): # ! finish documentation
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
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
        return info, response.json()
    except KeyError:
        logging.warning("No Rules Found!")
        return None, response.json()


def delete_all_stream_rules(rules_response):
    if rules_response is None or "data" not in rules_response:
        return None

    ids = list(map(lambda rule: rule["id"], rules_response["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    logging.debug(f"Rule Deletion Response: {json.dumps(response.json())}")

def delete_stream_rules(ids):
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    logging.debug(f"Rule Deletion Response: {json.dumps(response.json())}")

def set_stream_rules(rules):
    payload = {"add":rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
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

def connect_to_stream():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )

    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    with open("texts.txt","a+") as f: # ! make this stream into a python object as well
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                print(json.dumps(json_response, indent=4, sort_keys=True))
                f.write(json_response["data"]["text"])

if __name__ == '__main__':
    # sami_id = get_user_id("Sami_Amer_PS")["id"]
    # sami_tweets = get_user_timeline(sami_id)
    # connect_to_stream()

    rules = [
        {"value": "from:2899773086"},
        {"value": "from:Wario64"},
        {"value": "from:763099930487615488 "}]
    
    # rules, response = get_stream_rules()
    # delete_all_stream_rules(response)
    
    # set_stream_rules(rules)
