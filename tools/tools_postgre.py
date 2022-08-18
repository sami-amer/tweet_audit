# native
import logging
import os

# packages
import psycopg
import psycopg.sql as psql

# lib
from classes.classesv2 import TwitterHandler
from classes import PG_ARGS


class Toolkit:
    def __init__(self, bearer_token, db_args):
        self.logger = self.create_loggers()
        self.handler = TwitterHandler(bearer_token, None, self.logger)
        self.db_args = db_args

        try:
            self.connection = psycopg.connect(**self.db_args)
            self.logger.info("Postgres Connection Established!")
        except Exception as err:
            self.connection = None
            self.logger.error("POSTGRES CONNECTION BROKEN")
            self.logger.error(db_args)
            self.logger.error(f"{err}")
            raise psycopg.errors.CannotConnectNow()

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
        if len(rules) > 25:
            self.logger.error(
                "RULES ARE GREATER THAN 25! THIS IS NOT ALLOWED WITH ELEVATED ACCESS!"
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

    def update_user_rules(self, new_users: list) -> list[dict[str]]:
        old_rules, response = self.handler.get_rules()

        users = self.extract_users_from_rules(old_rules) if old_rules else []
        # print(f"Old Rules: {users}")
        new_users = [x for x in new_users if x not in users]
        users += new_users
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.logger.debug(f"{self.handler.set_rules(rules)}")
        self.update_author_to_id()

        return rules  # only for testing purposes

    def set_user_rules(self, users):
        #! add automatic id - username mapping update
        old_rules, response = self.handler.get_rules()
        rules = self.format_rules(users)

        self.handler.delete_all_rules(response)
        self.handler.set_rules(rules)
        self.update_author_to_id()

    def update_author_to_id(self) -> None:
        """
        Gets users from the Twitter Stream, compares them to local id_name_mapping, and updates anything missing
        """
        info, resp = self.handler.get_rules()

        if not info:
            return
        # print(resp)
        # print(info)
        rules = [x["value"].split("OR") for x in info["rules"]]
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
            psql.SQL("SELECT user_name,user_id FROM {};").format(
                psql.Identifier("id_name_mapping")
            )
        )
        current_names = cur.fetchall()
        current_ids = (
            set([name[1] for name in current_names]) if current_names else set()
        )
        current_names = [name[0] for name in current_names] if current_names else set()

        self.logger.info("Got names from DB")
        names_set = set(current_names) if current_names else set()
        users_add = [name for name in users if name not in names_set]

        if not users_add:
            self.logger.info("No new users to add to mapping, skipping...")
            return

        for i in range(0, len(users_add), 100):

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
        to_update = []
        to_add = []
        for resp in flattened_responses:
            if int(resp[0]) in current_ids:
                to_update.append(resp)
            else:
                to_add.append(resp)

        conn = self.connection
        curr = conn.cursor()
        curr.executemany(
            psql.SQL("INSERT INTO {} VALUES (%s,%s,%s)").format(
                psql.Identifier("id_name_mapping")
            ),
            to_add,
        )
        conn.commit()
        for resp in to_update:
            resp = {"user_id": resp[0], "user_name": resp[1], "user_full_name": resp[2]}
            curr.execute(
                psql.SQL(
                    "UPDATE {} SET user_name=%(user_name)s, user_full_name=%(user_full_name)s WHERE user_id=%(user_id)s;"
                ).format(psql.Identifier("id_name_mapping")),
                resp,
            )
            conn.commit()

    def create_user_group_db(self, users: list[str], table_name: str) -> None:
        """
        creates a table with table_name and a simple list of users
        """
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

    def update_user_group_db(self, users: list[str], table_name: str) -> None:
        """
        Gets names from a user_db, compares them to the input users, and then adds the difference
        """
        conn = self.connection
        cur = conn.cursor()
        names = cur.execute(
            psql.SQL("SELECT user_name FROM {};").format(psql.Identifier(table_name))
        )
        names = cur.fetchall()
        names_set = {name[0] for name in names}
        users_add = [[name] for name in users if name not in names_set]

        cur.executemany(
            psql.SQL("INSERT INTO {} VALUES (%s);").format(psql.Identifier(table_name)),
            users_add,
        )

    def get_user_list(self, table_name: str) -> list:

        conn = self.connection
        cur = conn.cursor()
        cur.execute(
            psql.SQL("SELECT user_name FROM {};").format(psql.Identifier(table_name))
        )
        output = cur.fetchall()
        return [user[0] for user in output]

    def get_user_id(self, user: str) -> dict:
        """
        Given a twitter username without "@", returns the user id

        Arguments:
            user    (str): the twitter username without '@'
        """
        usernames = f"usernames={user}"
        user_fields = "user.fields=id,verified,description,created_at"

        url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
        data = self.handler.get_from_endpoint(url)

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

    def download_user_mapping(self):
        user_mapping = {}

        conn = self.connection
        cur = conn.cursor()
        try:
            cur.execute(
                psql.SQL("SELECT user_id,user_name FROM {};").format(
                    psql.Identifier("id_name_mapping")
                )
            )
        except Exception as err:
            self.logger.error(f"ERROR DOWNLOADING USER MAPPING {err}")
        try:
            user_data = cur.fetchall()
        except:
            user_data = None
        if user_data == None:
            self.logger.warning("id_name_mapping Empty. Is this expected?")
            return
        for data in user_data:
            user_mapping[data[0]] = data[1]
        self.logger.info("User Mapping Downloaded Successfully!")
        return user_mapping

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

    def initialize_db(self) -> None:
        """
        Creates the tweet table and the id_name_mapping table.
        Also adds a test into the tweet table to make sure all is well.
        """
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
            conn.commit()

        except psycopg.errors.DuplicateTable:
            self.logger.warning("DUPLICATE TABLE, id_name_mapping EXISTS")
            conn.rollback()

        except psycopg.errors.InFailedSqlTransaction as e:
            self.logger.error(f"Fatal Error: {e}")
            conn.rollback()
            raise psycopg.errors.InFailedSqlTransaction

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
            conn.commit()

        except psycopg.errors.DuplicateTable:
            self.logger.warning("DUPLICATE TABLE, tweets EXISTS")
            conn.rollback()

        except psycopg.errors.InFailedSqlTransaction as e:
            self.logger.error(f"Fatal Error: {e}")
            conn.rollback()
            raise psycopg.errors.InFailedSqlTransaction

        try:
            cur.execute(
                psql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s);").format(
                    psql.Identifier("tweets")
                ),
                (1, 1, "testName", "testText"),
            )
            conn.commit()
        except psycopg.errors.UniqueViolation:
            self.logger.warning(
                "test tweet is already in database! Are you re-initializing?"
            )
            conn.rollback()

        conn.commit()

    def test_connection(self, secret=True) -> None:
        """
        Tests connection to the postgresql server by looking for a tweet with id=1
        This tweet is added during initialize_db()
        Also prints out which envrionment variables are found; if secret=FALSE, prints the variables to the log
        """
        self.logger.info("Checking environment variables")

        env_vars = [
            "POSTGRES_HOST",
            "POSTGRES_DBNAME",
            "POSTGRES_USER",
            "POSTGRES_PASS",
            "BEARER_TOKEN",
        ]
        self.logger.info(os.environ)
        for env_var in env_vars:
            self.logger.info(f"Looking for {env_var}...")

            if os.environ.get(env_var):
                self.logger.info("FOUND!")
                if not secret:
                    self.logger.info(
                        f"NON-SECRET MODE: value of {env_var} is {os.environ.get(env_var)}"
                    )
            else:
                self.logger.warning(f"Environment variable {env_var} was NOT FOUND!")

        self.logger.info("Testing Connection to Postgres Server")

        conn = self.connection
        cur = conn.cursor()
        cur.execute(
            psql.SQL("SELECT tweet_id FROM {} WHERE tweet_id=1;").format(
                psql.Identifier("tweets")
            )
        )
        conn.commit()

        self.logger.info("Connection to Postgres server good!")
        self.logger.info("Connection test complete.")

    def table_exists(self, table_name: str):
        conn = self.connection
        cur = conn.cursor()
        cur.execute(
            "select exists(select * from information_schema.tables where table_name=%s)",
            (table_name,),
        )
        return cur.fetchone()[0]

    def add_users(self, users: list[str], table_name: str) -> None:
        """
        If a table with table_name exists, updates the table. Otherwsie creates the table and updates.
        """
        self.logger.info(f"Adding {len(users)} users to {table_name}")
        if not self.table_exists(table_name):
            self.create_user_group_db(users, table_name)
        else:
            self.update_user_group_db(users, table_name)

        self.update_author_to_id()
        self.update_user_rules(users)
        self.sync_users(table_name)

    def sync_users(self, table_name) -> None:
        """
        Compares local table and remote stream, and updates the users.
        Is run on a per-table basis
        """

        #  grab users from local sql
        table_local_users = set(self.get_user_list(table_name))
        #  grab users from stream
        old_rules, response = self.handler.get_rules()
        stream_users = (
            set(self.extract_users_from_rules(old_rules)) if old_rules else set()
        )
        #  Compare and find most efficient way to update (need a func and algo just for this)
        to_add = table_local_users - stream_users
        global_local_users = set(self.download_user_mapping().values())
        to_delete = stream_users - global_local_users

        if to_add:
            self.add_users(to_add, table_name)
        if to_delete:
            self.remove_users_from_rules(to_delete)

    def cache_users_local(self, cache_path):
        users = [str(user) for user in self.download_user_mapping().values()]
        with open(cache_path, "w+") as f:
            for user in users:
                f.write(user + "\n")

    def read_from_cache(self, cache_path):
        users = []
        with open(cache_path, "r") as f:
            for line in f:
                users.append(line.strip())

        return users


if __name__ == "__main__":

    bearer_token = os.environ.get("BEARER_TOKEN")
    db_args = PG_ARGS

    kit = Toolkit(bearer_token, db_args)
    # news = kit.read_from_cache("news.bak")
    # senators = kit.read_from_cache("senators.bak")
    # house = kit.read_from_cache("house.bak")

    # kit.initialize_db()
    kit.test_connection()
    # kit.add_users(news, "news_orgs")
    # kit.add_users(senators, "us_senate")
    # kit.add_users(house, "us_house")
    # kit.cache_users_local("local_user.txt")

    # print(kit.handler.get_rules())
