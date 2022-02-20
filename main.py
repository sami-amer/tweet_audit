"""
Full pipeline control for tweet audit
""" 
import requests
import os
import json

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
    response = requests.request("GET", url, auth=bearer_oauth,params=params)
    print(response.status_code)
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

def get_user_timeline(user_id):
    url= "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    params = {"tweet.fields": "text"}
    data = connect_to_endpoint(url, params)['data'][0]
    info = {
        "id" : data["id"],
        "text": data["text"]
    }
    return info

if __name__ == '__main__':
    sami_id = get_user_id("Sami_Amer_PS")["id"]
    sami_tweets = get_user_timeline(sami_id)
