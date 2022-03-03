import requests
import os
import json
import logging, sqlite3
import pandas as pd
from classes import TwitterHandler

#! add proper logging here
bearer_token = os.environ.get("BEARER_TOKEN")


def format_rules(self,usernames): # ! Use Usernames over User IDs because they are capped at 15
    rules = []
    for i in range(0,len(usernames),22): # ! if this can find average, can be more efficient
        # ! Add check for abnormal lengths
        rule = ''
        for user in usernames[i:i+22]:
            rule+=f"from:{user} OR "
        rules.append(rule[:-4:])
    return rules

def clean_user_rule(user_rule):
    return user_rule.strip()[5:]

def update_author_to_id():
    """Uses a large amount of API Calls. Need to insert timeout if lists are larger than 900
    Rate limits are 900 per 15 minutes, no matter the access level"""
    handler = TwitterHandler(bearer_token,logging.getLogger())
    rules = [x["value"].split('OR') for x in handler.get_rules()[0]['rules']]
    # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
    flattened_rules = [item for rule in rules for item in rule]
    # ----
    users = [clean_user_rule(rule) for rule in flattened_rules]
    print(f"Number of users is: {len(users)}")
    # ! add a check here for number of users
    get_rules = []
    responses = []
    for i in range(0,len(users),100):
        # get_rule = users[i:i+1].join(",")
        get_rule = ",".join(users[i:i+101])
        get_rules.append(get_rule)

    for i in get_rules:
        # usernames = "usernames="+i
        # # print(usernames)
        # user_fields = "user.fields=id,name"
        # url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
        # response = handler.get_from_endpoint(url)
        response = handler.get_user_id()
        responses.append(response)
    
    flattened_responses = [(item["id"],item["username"],item["name"]) for response in responses for item in response["data"]]
    # print(flattened_responses)
    db = sqlite3.connect("test.db")

    db.executemany("INSERT INTO ID_NAME_MAPPING VALUES (?,?,?)", flattened_responses)
    db.commit()
    db.close()


if __name__ == '__main__':
    # text = """from:SenatorBaldwin OR from:SenJohnBarrasso OR from:SenatorBennet OR from:MarshaBlackburn OR from:SenBlumenthal OR from:RoyBlunt OR from:CoryBooker OR from:JohnBoozman OR from:SenatorBraun OR from:SenSherrodBrown OR from:SenatorBurr OR from:SenatorCantwell OR from:SenCapito OR from:SenatorCardin OR from:SenatorCarper OR from:SenBobCasey OR from:SenBillCassidy OR from:SenatorCollins OR from:ChrisCoons OR from:JohnCornyn OR from:SenCortezMasto OR from:SenTomCotton"""
    update_author_to_id()
