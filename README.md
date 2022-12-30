# Tweet Auditor
Using Distributed Ledgers to Hold Public Figures and Institutions Accountable

## What is this?

At its core, this project aims to allow users to easily choose a list of public officials whose tweets they want to track, and automate the process of following and committing those tweets to a local database. Additionally, the project is built using a local blockchain implementation ([hyperledger sawtooth](https://www.hyperledger.org/use/sawtooth)), as a proof of concept for using this auditor as a larger database of public official tweets. For more about the philosphy behind this project, as well as the process from start to end, feel free to check out my blog post [here](https://blog.sami.ps/twitter-auditor#heading-immutable). The below "how-to" is excerpted from this post!

## How To

Pre-compiled binaries are not available, but building from source should be straightforward. Before going through with the following steps, please keep in mind that the docker-compose will launch at minimum five processes: the main validation node, the tweet transaction processor, the REST API, the Sawtooth CLI, and the consensus engine (this engine is set to dev-mode by default, which is only good for testing. Please switch to a different engine, like PBFT or PoET before deploying). I would recommend only running this on devices with ample RAM (16GB+) and good multi-threading (minimum 4 threads, preferably 8). Of course, you are free to try this on any machine you like, the worst that will happen is the docker image will fail (as far as I know).

### The Steps

1. Clone the repo
1. Download and install Rust and Docker
1. Get an API key for the Twitter API, and understand the filtered stream endpoint. Be wary of rate limits!
1. Set up a PostgreSQL server (for macOS I recommend Postgres.app, and for Linux I recommend pgAdmin)
1. Set the environment variables (see the Server Template toml)
1. Compile release versions of the rust-sawtooth-client, rust-sawtooth, and rust-streamer
1. Compose the docker file and start it
1. Run rust-streamer
1. Voila! You should be able to see the blocks of the distributed ledger by going to localhost:8008/blocks
1. Using the Python API to Control the Auditor

Now that you have the docker container up and running, you can use the Python API to create, edit, and delete user lists, both within the SQL server and the Twitter FilteredStream. Since there is currently no CLI, you will need to either make a new python file and import tools_postgre, or put your code directly into tools_postgres.py's __main__ block.

The first thing you need to do is to create a Toolkit() object, with the Twitter API Bearer Token as the first argument and the server TOML you created above as the second. From there, all of your interactions with the DB and the stream will come through Toolkit() method calls. Next, you can initialize the DB with the aptly named kit.initialize_db which creates a simple table of tweets and a table mapping a Twitter user's ID to their username. This method also creates a dummy tweet that other methods in the program check for when determining the health of the server.

To make sure you initialized the variables correctly, run kit.test_connection(False), which will check the environment variables you set and spit them back at you, so you can verify that they are what you want them to be (if you want it to also show you the bearer token, call it with the True argument). This function also checks for the aforementioned dummy tweet to make sure changes are being committed to the local server.

Lists of users are made to be split up into different tables per category, but this is not a hard rule and can be side-stepped if you do not need that organization. To make a user table, simply use the kit.create_user_db() method with a list of names and a name for the DB. For simplicity, you can use the kit.read_from_cache() method to create a Python list from a text file with one username per line. You can also do the opposite and make this file from a Python list using kit.cache_users_local().

Now that you have a table of users, you can run kit.sync_users() to automatically compare the local table to the online list of users, and add them to the Twitter stream! Keep in mind that the sync works on a per-table basis only, so if you want to control a subset of users on their own, I would recommend splitting them up into their own table.

With your rules added, your docker container running, and rust-streamer doing its thing, you should start seeing the tweets rolling in! You can check both the Postgres database as well as the hyperledger blockchain to ensure that things are being saved correctly; if something is not as it should be, double-check the logs to make sure all is well.

