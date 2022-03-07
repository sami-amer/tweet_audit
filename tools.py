import requests, pickle
import os
import logging, sqlite3
import pandas as pd
from classes import TwitterHandler

class Toolkit:

    def __init__(self,bearer_token):
        self.logger = self.create_loggers()
        self.handler = TwitterHandler(bearer_token,self.logger)

    def format_rules(self, usernames):
        sorted_users = sorted(usernames,key= len)
        rules = []
        curr_rule = ''
        counter = 0
        for name in sorted_users:
            counter += 1
            if len(curr_rule) + 5 +len(name) >= 512:
                curr_rule = curr_rule[:-4]
                rules.append(curr_rule)
                curr_rule = ''
            curr_rule+= f"from:{name} OR "
        curr_rule = curr_rule[:-4]
        rules.append(curr_rule)
        rules = [{"value": x, "tag":str(len(x))} for x in rules]
        if len(rules) > 5:
            self.logger.error("RULES ARE GREATER THAN 5! THIS IS NOT ALLOWED WITH ESSENTIAL ACCESS!")
        return(rules)


    def update_user_rules(self,new_users):
        #! add automatic id - username mapping update
        old_rules, response = self.handler.get_rules()

        users = self.extract_users_from_rules(old_rules) if old_rules else []
        print(f"Old Rules: {users}")
        users += new_users
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.handler.set_rules(rules)

    def set_user_rules(self, users):
        #! add automatic id - username mapping update
        old_rules, response = self.handler.get_rules()
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.handler.set_rules(rules)
        
    def extract_users_from_rules(self,rules):
        rules = [x["value"].split('OR') for x in rules['rules']]
        # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        flattened_rules = [item for rule in rules for item in rule]
        # ----
        users = [self.clean_user_rule(rule) for rule in flattened_rules]
        return users

    def clean_user_rule(self,user_rule):
        return user_rule.strip()[5:]

    def update_author_to_id(self,db_path):
        #! needs revamping to not add duplicates

        rules = [x["value"].split('OR') for x in self.handler.get_rules()[0]['rules']]
        # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        flattened_rules = [item for rule in rules for item in rule]
        # ----
        users = [self.clean_user_rule(rule) for rule in flattened_rules]
        print(f"Number of users is: {len(users)}")
        # ! add a check here for number of users
        get_rules = []
        responses = []
        for i in range(0,len(users),100):
            # get_rule = users[i:i+1].join(",")
            get_rule = ",".join(users[i:i+101])
            get_rules.append(get_rule)

        for i in get_rules:
            response = self.handler.get_user_id()
            responses.append(response)
        
        flattened_responses = [(item["id"],item["username"],item["name"]) for response in responses for item in response["data"]]

        db = sqlite3.connect(db_path)

        db.executemany("INSERT INTO ID_NAME_MAPPING VALUES (?,?,?)", flattened_responses)
        db.commit()
        db.close()

    @staticmethod
    def create_loggers() -> logging.Logger:
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
        return log_tools


    def add_user_group_db(self,users, table_name,db_path):
        pass

    def update_user_group_db(self,users,table_name,dp_path):
        pass

    
if __name__ == '__main__':
    # --- Code to load pickle and usernames from df
    # with open("_data/pickles/rule_dict","r") as f:
        # rules = pickle.load(f)
        # json.dump(rules,f)
        # rules = json.load(f)
    # df = pd.read_csv("_data/senate_usernames_dec21.csv")
    # usernames = df["username"].to_list()
    # usernames += ["nytimes","KyivIndependent","RT_com","RT_America"]
    bearer_token = os.environ.get("BEARER_TOKEN")
    kit = Toolkit(bearer_token)
