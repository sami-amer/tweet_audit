import requests, pickle
import os
import logging, psycopg
import psycopg.sql as psql
import pandas as pd
from classes.classesv1 import TwitterHandler


class Toolkit:
    def __init__(self, bearer_token, db_args):
        self.logger = self.create_loggers()
        self.handler = TwitterHandler(bearer_token, self.logger)
        self.db_args = db_args

        try:
            self.connection = psycopg.connect(**self.db_args)
            self.logger.info("Postgres Connection Established!")
        except Exception as err:
            self.connection = None
            self.logger.error("POSTGRES CONNECTION BROKEN")
            self.logger.error(db_args)
            self.logger.error(f"{err}")

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

    def tearDown(self):
        self.connection.close()

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

    def clean_user_rule(self, user_rule):
        return user_rule.strip()[5:]

    def extract_users_from_rules(self, rules):
        rules = [x["value"].split("OR") for x in rules["rules"]]
        # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        flattened_rules = [item for rule in rules for item in rule]
        # ----
        users = [self.clean_user_rule(rule) for rule in flattened_rules]
        return users

    def remove_users_from_rules(self, users_to_remove):
        old_rules, response = self.handler.get_rules()

        users = self.extract_users_from_rules(old_rules) if old_rules else []
        self.logger.info(f"Old Rules: {users}")
        for user in users_to_remove:
            users.remove(user)
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.handler.set_rules(rules)
        return rules  # only for testing purposes

    def update_user_rules(self, new_users: list):
        #! add automatic id - username mapping update
        old_rules, response = self.handler.get_rules()

        users = self.extract_users_from_rules(old_rules) if old_rules else []
        # print(f"Old Rules: {users}")
        new_users = [x for x in new_users if x not in users]
        users += new_users
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.handler.set_rules(rules)
        return rules  # only for testing purposes

    def set_user_rules(self, users):
        #! add automatic id - username mapping update
        old_rules, response = self.handler.get_rules()
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.handler.set_rules(rules)

    def update_author_to_id(self):

        rules = [x["value"].split("OR") for x in self.handler.get_rules()[0]["rules"]]
        # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        flattened_rules = [item for rule in rules for item in rule]
        # ----
        users = [self.clean_user_rule(rule) for rule in flattened_rules]
        self.logger.info(f"Number of users is: {len(users)}")
        # ! add a check here for number of users
        get_rules = []
        responses = []

        conn = self.connection
        cur = conn.cursor()
        current_names = cur.execute(
            psql.SQL("SELECT USER_NAME FROM {};").format(
                psql.Identifier("id_name_mapping")
            )
        )
        current_names = cur.fetchall()
        current_names = [name[0] for name in current_names] if current_names else None

        self.logger.info("Got names from DB")
        names_set = set(current_names) if current_names else set()
        users_add = [name for name in users if name not in names_set]
        if not users_add:
            self.logger.info("No new users to add to mapping, skipping...")
            return
        for i in range(0, len(users_add), 100):
            # get_rule = users_add[i:i+1].join(",")

            get_rule = ",".join(users_add[i : i + 100])
            get_rules.append(get_rule)

        # self.logger.info("Got rules from endpoint")

        for i in get_rules:
            response = self.get_user_id(i)
            responses.append(response)

        flattened_responses = [
            (item["id"], item["username"], item["name"])
            for response in responses
            for item in response["data"]
        ]

        conn = self.connection
        curr = conn.cursor()
        curr.executemany(
            psql.SQL("INSERT INTO {} VALUES (%s,%s,%s)").format(
                psql.Identifier("id_name_mapping")
            ),
            flattened_responses,
        )
        conn.commit()

    def create_user_group_db(self, users: list[str], table_name: str) -> None:
        users_add = [[user] for user in users]
        conn = self.connection
        cur = conn.cursor()
        cur.execute(
            psql.SQL(
                """CREATE TABLE {} (user_name TEXT PRIMARY KEY NOT NULL);"""
            ).format(psql.Identifier(table_name))
        )
        # cur.execute(psql.SQL("INSERT INTO {} VALUES (%s)").format(psql.Identifier(table_name)), (10,))
        cur.executemany(
            psql.SQL("INSERT INTO {} VALUES (%s);").format(psql.Identifier(table_name)),
            users_add,
        )
        conn.commit()

    def update_user_group_db(self, users, table_name):

        conn = self.connection
        cur = conn.cursor()
        names = cur.execute(
            psql.SQL("SELECT user_name FROM {};").format(psql.Identifier(table_name))
        )
        names = cur.fetchall()
        names_set = set(names)
        users_add = [[name] for name in users if name not in names_set]

        cur.executemany(
            psql.SQL("INSERT INTO {} VALUES (%s);").format(psql.Identifier(table_name)),
            users_add,
        )

    def get_user_list(self, table_name):

        conn = self.connection
        cur = conn.cursor()
        cur.execute(
            psql.SQL("SELECT user_name FROM {};").format(psql.Identifier(table_name))
        )
        output = cur.fetchall()
        return (
            output  #! Do I want this to output solo tuples? Fix tests if not the case
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
        # with self.connection as conn:
        conn = self.connection
        cur = conn.cursor()
        try:
            cur.execute(
                psql.SQL(
                    """CREATE TABLE {} (
                user_id BIGINT PRIMARY KEY NOT NULL,
                user_name TEXT NOT NULL,
                user_full_name TEXT);"""
                ).format(psql.Identifier("id_name_mapping"))
            )
        except psycopg.errors.DuplicateTable:
            self.logger.warning("DUPLICATE TABLE, id_name_mapping EXISTS")
        try:
            cur.execute(
                psql.SQL(
                    """CREATE TABLE {} (
                tweet_id BIGINT PRIMARY KEY NOT NULL,
                author_id BIGINT NOT NULL,
                author_name TEXT NOT NULL,
                tweet_text TEXT NOT NULL);"""
                ).format(psql.Identifier("tweets"))
            )
        except psycopg.errors.DuplicateTable:
            self.logger.warning("DUPLICATE TABLE, tweets EXISTS")
        conn.commit()

        cur.execute(
            psql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s);").format(
                psql.Identifier("tweets")
            ),
            (1, 1, "testName", "testText"),
        )
        conn.commit()

    def test_connection(self):
        conn = self.connection
        cur = conn.cursor()
        cur.execute(
            psql.SQL("SELECT tweet_id FROM {} WHERE tweet_id=1;").format(
                psql.Identifier("tweets")
            )
        )
        conn.commit()


if __name__ == "__main__":
    # --- Code to load pickle and usernames from df
    # with open("_data/pickles/rule_dict","r") as f:
    # rules = pickle.load(f)
    # json.dump(rules,f)
    # rules = json.load(f)

    # df = pd.read_csv("_data/senate_usernames_dec21.csv")
    # senators = df["username"].to_list()

    american_news = ["AP", "WhiteHouse", "FoxNews", "CNN", "potus", "msnbc"]

    bearer_token = os.environ.get("BEARER_TOKEN")
    db_args = {"host": "localhost", "dbname": "template1", "user": "postgres"}

    # kit = Toolkit(bearer_token, "test.db")
    kit = Toolkit(bearer_token, db_args)
    print(kit.connection)

    # kit.initialize_db()
    # kit.update_author_to_id()
    # kit.create_user_group_db(senators,"us_senators")

    # kit.update_user_rules(american_news)
    # kit.update_user_group_db(american_news,"american_news")
    # kit.update_author_to_id()
    #! WRAP THESE INTO ONE FUNC ^
    #! ADD UPDATE OR DELETE FUNCTION FOR DB
    #! IMPLEMENT NEW CHANGES FOR SQLLITE

    # print(kit.handler.get_rules())

    # kit.remove_users_from_rules(["senatemajldr"])
    # ! ADD CONNECTION CLOSES OR TRY EXCEPT FINALYS FOR CONNECTIONS
    # kit.update_author_to_id()
    # kit.update_user_rules
    # for user in usernames:
    #     print(kit.get_user_id(user))
    # kit.add_user_group_db(usernames, "NEWS_USER_NAMES")
    # kit.update_user_group_db(usernames, "NEWS_USER_NAMES")
