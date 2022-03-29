import toml


__author__ = "Sami Amer"
__copyright__ = "Copyright 2022, Sami Amer"
__credits__ = ["Sami Amer"]
__license__ = "GPL"
__version__ = "0.1.2"
__maintainer__ = "Sami Amer"
__email__ = "samiamer@mit.edu"
__status__ = "Development"

toml_dict = toml.load("classes/.server.toml")

POSTGRES_ARGS = toml_dict["postgres"]
SQLLITE_ARGS = toml_dict["sqllite"]
MAC_ARGS = toml_dict["postgres-mac"]
AWS_ARGS = toml_dict["AWS"]
