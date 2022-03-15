import requests, pickle
import os
import logging, psycopg2
import psycopg2.sql as psql
import pandas as pd
from classesv1 import TwitterHandler


class Toolkit:
    def __init__(self, bearer_token, db_args):
        self.logger = self.create_loggers()
        self.handler = TwitterHandler(bearer_token, self.logger)
        self.db_args = db_args

    def format_rules(self, usernames):
        sorted_users = sorted(usernames, key=len)
        rules = []
        curr_rule = ""
        counter = 0
        for name in sorted_users:
            counter += 1
            if len(curr_rule) + 5 + len(name) >= 512:
                curr_rule = curr_rule[:-4]
                rules.append(curr_rule)
                curr_rule = ""
            curr_rule += f"from:{name} OR "
        curr_rule = curr_rule[:-4]
        rules.append(curr_rule)
        rules = [{"value": x, "tag": str(len(x))} for x in rules]
        if len(rules) > 5:
            self.logger.error(
                "RULES ARE GREATER THAN 5! THIS IS NOT ALLOWED WITH ESSENTIAL ACCESS!"
            )
        return rules

    def update_user_rules(self, new_users):
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

    def extract_users_from_rules(self, rules):
        rules = [x["value"].split("OR") for x in rules["rules"]]
        # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        flattened_rules = [item for rule in rules for item in rule]
        # ----
        users = [self.clean_user_rule(rule) for rule in flattened_rules]
        return users

    def clean_user_rule(self, user_rule):
        return user_rule.strip()[5:]

    def update_author_to_id(self):
        #! needs revamping to not add duplicates

        rules = [x["value"].split("OR") for x in self.handler.get_rules()[0]["rules"]]
        # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        flattened_rules = [item for rule in rules for item in rule]
        # ----
        users = [self.clean_user_rule(rule) for rule in flattened_rules]
        print(f"Number of users is: {len(users)}")
        # ! add a check here for number of users
        get_rules = []
        responses = []
        for i in range(0, len(users), 100):
            # get_rule = users[i:i+1].join(",")
            get_rule = ",".join(users[i : i + 100])
            get_rules.append(get_rule)
        # print(get_rules)
        for i in get_rules:
            response = self.get_user_id(i)
            responses.append(response)

        flattened_responses = [
            (item["id"], item["username"], item["name"])
            for response in responses
            for item in response["data"]
        ]

        # print(flattened_responses)

        conn = psycopg2.connect(**self.db_args)
        curr = conn.cursor()
        curr.executemany(
            psql.SQL("INSERT INTO {} VALUES (%s,%s,%s)").format(
                psql.Identifier("ID_NAME_MAPPING")
            ),
            flattened_responses,
        )
        conn.commit()
        conn.close()

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

        log_tools = logging.getLogger("Tools")

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        fh_tools = logging.FileHandler("logs/TOOLS_LOG.log")
        fh_tools.setFormatter(formatter)

        log_tools.addHandler(fh_tools)
        log_tools.addHandler(ch)
        return log_tools

    def add_user_group_db(self, users: list[str], table_name: str) -> None:
        users_add = [[user] for user in users]
        with psycopg2.connect(**self.db_args).cursor() as cur:
            cur.execute(
                psql.SQL(
                    """CREATE TABLE {} (USER_NAME STRING PRIMARY KEY NOT NULL);"""
                ).format(psql.Identifier(table_name))
            )
            # cur.execute(psql.SQL("INSERT INTO {} VALUES (%s)").format(psql.Identifier(table_name)), (10,))
            cur.executemany(
                psql.SQL("INSERT INTO {} VALUES (%s);").format(
                    psql.Identifier(table_name)
                ),
                users_add,
            )

    def update_user_group_db(self, users, table_name):
        with psycopg2.connect(**self.db_args).cursor as cur:
            names = cur.execute(
                psql.SQL("SELECT USER_NAME FROM {};").format(
                    psql.Identifier(table_name)
                )
            )
            names = [name[0] for name in names]
        names_set = set(names)
        users_add = [[name] for name in users if name not in names_set]
        with psycopg2.connect(**self.db_args).cursor() as cur:
            cur.executemany(
                psql.SQL("INSERT INTO {} VALUES (?);").format(
                    psql.Identifier(table_name)
                ),
                users_add,
            )

    def get_user_id(self, user: str) -> dict:
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
        data = self.handler.get_from_endpoint(url)
        # data = self.handler.get_from_endpoint(url)["data"][0]
        # print(data)
        # info = {
        #     "id": data["id"],
        #     "name": data["name"],
        #     "username": data["username"],
        #     "description": data["description"],
        #     "verified": data["verified"],
        #     "creation_date": data["created_at"],
        # }
        return data

    def get_user_timeline(self, user_id: str, max_results=5) -> list[dict]:
        """
        Given a user id, return some tweets from the users timeline

        Arguments:
            user_id (str): the users id
        """
        url = "https://api.twitter.com/2/users/{}/tweets".format(user_id)
        params = {
            "tweet.fields": "text,source,author_id,attachments",
            "max_results": str(max_results),
        }
        data = self.handler.get_from_endpoint(url, params)[
            "data"
        ]  # ? Make this a default dict?
        tweets = []
        for tweet in data:
            info = {
                "id": tweet["id"],
                "text": tweet["text"],
                "author_id": tweet["author_id"],  # ! Add check for this
                "source": tweet["source"],
                # "attachments": tweet["attachments"] if tweet["attachments"] else None ! add check for this
            }
            tweets.append(info)
        return tweets

    def get_user_from_tweet(self, id: str):
        tweet_fields = "tweet.fields=lang,author_id"
        # ids = "ids=1278747501642657792,1255542774432063488"
        id = f"ids={id}"
        url = "https://api.twitter.com/2/tweets?{}&{}".format(
            id, tweet_fields
        )  # ? Maybe use [text] response to double checK?

        data = self.handler.get_from_endpoint(url)
        user_id = data["data"][0]["author_id"]

        # print(json.dumps(data, indent=4, sort_keys=True))
        self.logger.info(f"User ID Query Returned: {user_id}")
        return user_id

    def initialize_db(self):
        with psycopg2.connect(**self.db_args) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    psql.SQL(
                        """CREATE TABLE {} (
                    USER_ID BIGINT PRIMARY KEY NOT NULL,
                    USER_NAME TEXT NOT NULL,
                    USER_FULL_NAME TEXT);"""
                    ).format(psql.Identifier("ID_NAME_MAPPING"))
                )
            except psycopg2.errors.DuplicateTable:
                self.logger.warning("DUPLICATE TABLE, ID_NAME_MAPPING EXISTS")
            try:
                cur.execute(
                    psql.SQL(
                        """CREATE TABLE {} (
                    TWEET_ID BIGINT PRIMARY KEY NOT NULL,
                    AUTHOR_ID BIGINT NOT NULL,
                    AUTHOR_NAME TEXT NOT NULL,
                    TWEET_TEXT TEXT NOT NULL);"""
                    ).format(psql.Identifier("TWEETS"))
                )
            except psycopg2.errors.DuplicateTable:
                self.logger.warning("DUPLICATE TABLE, TWEETS EXISTS")
            conn.commit()

            cur.execute(
                "INSERT INTO TWEETS VALUES (%s,%s,%s,%s);",
                (1, 1, "testName", "testText"),
            )
            conn.commit()

    def test_connection(self):
        with psycopg2.connect(**self.db_args) as conn:
            cur = conn.cursor()
            cur.execute("SELECT TWEET_ID FROM TWEETS WHERE TWEET_ID=1;")
            conn.commit()

    # def add_users(self,user_ids:list[tuple]) -> None:
    #     rules = []
    #     for id,tag in user_ids:
    #         rules.append({"value": f"from:{id}", "tag": f"{tag}"})

    #     for rule in rules:
    #         self.logger.info(f"Adding Rule: {rule}")
    #     self.handler.set_rules(rules)


if __name__ == "__main__":
    # --- Code to load pickle and usernames from df
    # with open("_data/pickles/rule_dict","r") as f:
    # rules = pickle.load(f)
    # json.dump(rules,f)
    # rules = json.load(f)
    df = pd.read_csv("_data/senate_usernames_dec21.csv")
    # usernames = df["username"].to_list()
    usernames = ["nytimes", "KyivIndependent", "RT_com", "RT_America"]
    bearer_token = os.environ.get("BEARER_TOKEN")
    db_args = {"host": "localhost", "database": "template1", "user": "postgres"}
    # kit = Toolkit(bearer_token, "test.db")
    kit = Toolkit(bearer_token, db_args)
    # kit.initialize_db()
    kit.update_author_to_id()
    # for user in usernames:
    #     print(kit.get_user_id(user))
    # kit.add_user_group_db(usernames, "NEWS_USER_NAMES")
    # kit.update_user_group_db(usernames, "NEWS_USER_NAMES")
