import requests, pickle
import os
import json
import logging, sqlite3
import pandas as pd
from classes import TwitterHandler

#! add proper logging here

formatter = logging.Formatter('%(asctime)s [%(name)s][%(levelname)s] %(message)s')
logging.basicConfig(level=logging.DEBUG, filename="logs/ROOT_LOG.log", format='%(asctime)s [%(name)s][%(levelname)s] %(message)s')

log_tools = logging.getLogger("Tools")

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

fh_tools = logging.FileHandler("logs/TOOLS_LOG.log")
fh_tools.setFormatter(formatter)

log_tools.addHandler(fh_tools)
log_tools.addHandler(ch)

bearer_token = os.environ.get("BEARER_TOKEN")



def clean_user_rule(user_rule):
    return user_rule.strip()[5:]

def update_author_to_id(db_path):

    handler = TwitterHandler(bearer_token, log_tools)
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
    db = sqlite3.connect(db_path)

    db.executemany("INSERT INTO ID_NAME_MAPPING VALUES (?,?,?)", flattened_responses)
    db.commit()
    db.close()


def format_rules(usernames):
    sorted_users = sorted(usernames,key= len)
    rules = []
    curr_rule = ''
    counter = 0
    for name in sorted_users:
        counter += 1
        if len(curr_rule) + 5 +len(name) >= 512:
            curr_rule = curr_rule[:-4]
            rules.append(curr_rule)
            # print(len(curr_rule))
            curr_rule = ''
        curr_rule+= f"from:{name} OR "
    curr_rule = curr_rule[:-4]
    rules.append(curr_rule)
    rules = [{"value": x, "tag":str(len(x))} for x in rules]
    return(rules)

def extract_users_from_rules(rules):
    rules = [x["value"].split('OR') for x in rules['rules']]
    # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
    flattened_rules = [item for rule in rules for item in rule]
    # ----
    users = [clean_user_rule(rule) for rule in flattened_rules]
    return users

def update_user_rules(new_users):
    handler = TwitterHandler(bearer_token, log_tools)
    old_rules, response = handler.get_rules()
    # print(old_rules)
    users = extract_users_from_rules(old_rules) if old_rules else []
    print(f"Old Rules: {users}")
    users += new_users
    rules = format_rules(users)
    # print(rules)
    handler.delete_all_rules(response)
    handler.set_rules(rules)


def add_user_group(users, table_name,db_path):
    pass

def update_user_group(users,table_name,dp_path):
    pass
if __name__ == '__main__':

    with open("_data/pickles/rule_dict","r") as f:
        # rules = pickle.load(f)
        # json.dump(rules,f)
        rules = json.load(f)

    # extracted = extract_users_from_rules(rules)
    df = pd.read_csv("_data/senate_usernames_dec21.csv")
    # usernames = df["username"].to_list()

    # new_rules = format_rules(extracted)
    # print(new_rules)
    # usernames += ["KyivIndependent","RT_America","RT_com"]
    usernames = ["nytimes"]
    update_user_rules(usernames)
