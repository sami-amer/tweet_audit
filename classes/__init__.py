import toml
import os


__author__ = "Sami Amer"
__copyright__ = "Copyright 2022, Sami Amer"
__credits__ = ["Sami Amer"]
__license__ = "GPL"
__version__ = "0.1.2"
__maintainer__ = "Sami Amer"
__email__ = "samiamer@mit.edu"
__status__ = "Development"

toml_dict = toml.load("classes/.server.toml")

PG_ARGS = toml_dict["postgres"]
T_ARGS = toml_dict["twitter"]

os.environ["POSTGRES_HOST"] = PG_ARGS["host"]
os.environ["POSTGRES_DBNAME"] = PG_ARGS["dbname"]
os.environ["POSTGRES_USER"] = PG_ARGS["user"]

if PG_ARGS["password"]:
    os.environ["POSTGRES_PASS"] = PG_ARGS["password"]

if T_ARGS["auth"]:
    os.environ["BEARER_TOKEN"] = T_ARGS["auth"]
