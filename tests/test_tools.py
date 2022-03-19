from sqlite3 import connect
from unittest.mock import Mock, patch
from pytest_postgresql import factories
import psycopg.sql as psql
import pytest, os, logging, time, psycopg
from tools.tools_postgre import Toolkit as ToolkitPostgre


formatter = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s] %(message)s")
log_tester = logging.getLogger("Tester")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

fh_tester = logging.FileHandler("logs/TESTER_LOG.log")
fh_tester.setFormatter(formatter)
log_tester.addHandler(fh_tester)
log_tester.addHandler(ch)


class fakeTwitterHandler:
    def __init__(self, logger) -> None:
        self.logger = logger
        self.responses = [
            {
                "data": {
                    "author_id": "247334603",
                    "id": "1501685993916841991",
                    "text": "As we develop climate policy, we must recognize the disproportionate impact natural disasters &amp; inaccessible resources have on women. This week, I joined @maziehirono to intro the Women &amp; Climate Change Act to ensure the US advances equitable climate solutions that work for all. https://t.co/nbWQJXPBo3",
                },
                "includes": {
                    "users": [
                        {
                            "id": "247334603",
                            "name": "Senator Dick Durbin",
                            "username": "SenatorDurbin",
                        },
                        {
                            "id": "92186819",
                            "name": "Senator Mazie Hirono",
                            "username": "maziehirono",
                        },
                    ]
                },
                "matching_rules": [{"id": "1500677568919392257", "tag": "501"}],
            },
            {
                "data": {
                    "author_id": "1099199839",
                    "id": "1501685742355066892",
                    "text": "RT @uspirg: Did you know that gas stoves can emit air pollutants in your home at levels exceeding EPA regulations for outdoor air quality?…",
                },
                "includes": {
                    "users": [
                        {
                            "id": "1099199839",
                            "name": "Martin Heinrich",
                            "username": "MartinHeinrich",
                        },
                        {"id": "42660729", "name": "U.S. PIRG", "username": "uspirg"},
                    ]
                },
                "matching_rules": [{"id": "1500677568919392261", "tag": "505"}],
            },
        ]

    def stream(self):
        # for response_line in self.responses:
        #     time.sleep(10)
        #     # if response_line:
        #     #     json_response = json.loads(response_line)
        #     self.logger.info("output fake json")
        #     yield response_line
        while True:
            time.sleep(10)
            yield self.responses[0]

    def get_rules():
        return {
            "rules": [
                {"value": "from:test1 OR from:test2 OR from:test3"},
                {"value": "from:test4 OR from:test5"},
            ]
        }, None

    def delete_all_rules(rules):
        return rules

    def set_rules(rules):
        return rules


postgresql_my_proc = factories.postgresql_proc()
postgresql = factories.postgresql("postgresql_my_proc")


class FakeObject(object):
    pass


def fake_extract_users_from_old_rules(rules):
    rules = [x["value"].split("OR") for x in rules["rules"]]
    # --- from https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
    flattened_rules = [item for rule in rules for item in rule]
    # ----
    users = [fake_clean_user_rule(rule) for rule in flattened_rules]
    return users


def fake_clean_user_rule(user_rule):
    return user_rule.strip()[5:]
    # return ToolkitPostgre.clean_user_rule(None,user_rule)


def fake_format_rules(rules):
    return ToolkitPostgre.format_rules(None, rules)


def fake_get_user_id(i):
    splits = i.split(",")
    user_ids = [s[-1] for s in splits]
    user_names = [s[:-1] for s in splits]
    full_names = ["Mr." + s[:-1] for s in splits]
    dicts = [
        {"id": x[0], "username": x[1], "name": x[2]}
        for x in zip(user_ids, user_names, full_names)
    ]
    return {"data": dicts}


def test_init(postgresql):
    connection = postgresql
    cur = connection.cursor()

    fake_self = FakeObject()
    fake_self.connection = connection
    ToolkitPostgre.initialize_db(fake_self)
    cur.execute(
        psql.SQL("SELECT tweet_id FROM {} WHERE tweet_id=1;").format(
            psql.Identifier("tweets")
        )
    )

    assert 1 == cur.fetchone()[0]


def test_update_author_id(postgresql):
    connection = postgresql
    cur = connection.cursor()

    fake_self = FakeObject()
    fake_self.connection = connection
    fake_self.handler = fakeTwitterHandler
    fake_self.logger = log_tester
    fake_self.clean_user_rule = fake_clean_user_rule
    fake_self.get_user_id = fake_get_user_id

    ToolkitPostgre.initialize_db(fake_self)
    ToolkitPostgre.update_author_to_id(fake_self)
    cur.execute(
        psql.SQL("SELECT user_id FROM {};").format(psql.Identifier("id_name_mapping"))
    )
    output = [x[0] for x in cur.fetchall()]

    assert output == [1, 2, 3, 4, 5]


def test_create_user_group_db(postgresql):
    connection = postgresql
    cur = connection.cursor()

    fake_self = FakeObject()
    fake_self.connection = connection
    fake_self.handler = fakeTwitterHandler

    users = ["sami", "wami", "bami"]
    table_name = "nicknames"
    ToolkitPostgre.create_user_group_db(fake_self, users, table_name)

    cur.execute(
        psql.SQL("SELECT user_name FROM {};").format(psql.Identifier(table_name))
    )
    output = [x[0] for x in cur.fetchall()]

    assert output == users


def test_update_user_group_db(postgresql):
    connection = postgresql
    cur = connection.cursor()

    fake_self = FakeObject()
    fake_self.connection = connection
    fake_self.handler = fakeTwitterHandler

    users = ["sami", "wami", "bami"]
    to_add = ["samasimi"]
    table_name = "nicknames"
    ToolkitPostgre.create_user_group_db(fake_self, users, table_name)
    ToolkitPostgre.update_user_group_db(fake_self, to_add, table_name)

    cur.execute(
        psql.SQL("SELECT user_name FROM {};").format(psql.Identifier(table_name))
    )
    output = [x[0] for x in cur.fetchall()]

    assert output == ["sami", "wami", "bami", "samasimi"]


def test_get_user_list(postgresql):
    connection = postgresql
    cur = connection.cursor()

    fake_self = FakeObject()
    fake_self.connection = connection
    fake_self.handler = fakeTwitterHandler

    users = ["sami", "wami", "bami"]
    table_name = "nicknames"
    ToolkitPostgre.create_user_group_db(fake_self, users, table_name)

    output = [x[0] for x in ToolkitPostgre.get_user_list(fake_self, table_name)]

    assert output == users


def test_format_rules():
    fake_self = FakeObject()
    fake_self.logger = log_tester
    names = ["testerPerson"] * 50
    formatted_rules = ToolkitPostgre.format_rules(fake_self, names)

    str1 = "from:testerPerson OR " * 24
    str2 = "from:testerPerson OR " * 24

    assert formatted_rules[0] == {
        "tag": str(len(str1[:-4])),
        "value": str1[:-4],
    } and formatted_rules[1] == {"tag": str(len(str2[:-4])), "value": str2[:-4]}


def test_clean_user_rule():
    fake_self = FakeObject()
    user_rule = " from:testRule "
    output = ToolkitPostgre.clean_user_rule(fake_self, user_rule)

    assert output == "testRule"


@patch.object(ToolkitPostgre, "clean_user_rule", fake_clean_user_rule)
def test_extract_users_from_old_rules():
    fake_self = FakeObject()
    fake_self.clean_user_rule = fake_clean_user_rule
    rules = {
        "rules": [
            {"value": "from:test1 OR from:test2 OR from:test3"},
            {"value": "from:test4 OR from:test5"},
        ]
    }
    output = ToolkitPostgre.extract_users_from_rules(fake_self, rules)
    assert output == ["test1", "test2", "test3", "test4", "test5"]


@patch.object(ToolkitPostgre, "clean_user_rule", fake_clean_user_rule)
# @patch.object(ToolkitPostgre,"format_rules",ToolkitPostgre.format_rules)
def test_remove_users_from_rules():

    fake_self = FakeObject()
    fake_self.handler = fakeTwitterHandler
    fake_self.extract_users_from_rules = fake_extract_users_from_old_rules
    fake_self.logger = log_tester
    fake_self.format_rules = fake_format_rules

    users_to_remove = ["test1", "test2"]

    output = ToolkitPostgre.remove_users_from_rules(fake_self, users_to_remove)
    assert output == [{"tag": "38", "value": "from:test3 OR from:test4 OR from:test5"}]


def test_update_user_rules():
    fake_self = FakeObject()
    fake_self.handler = fakeTwitterHandler
    fake_self.extract_users_from_rules = fake_extract_users_from_old_rules
    fake_self.logger = log_tester
    fake_self.format_rules = fake_format_rules

    users_to_add = ["test1", "test2"]

    output = ToolkitPostgre.update_user_rules(fake_self, users_to_add)
    assert output == [
        {
            "value": "from:test1 OR from:test2 OR from:test3 OR from:test4 OR from:test5",
            "tag": "66",
        }
    ]


if __name__ == "__main__":
    # postgresql = testing.postgresql.Postgresql(port=7654)
    # conn = psycopg2.connect(postgresql.url())
    # cur = conn.cursor()
    pass
