use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bytes::Bytes;
use streamer_utils::ledger::add_to_ledger;
use core::panic;
use log;
use reqwest;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, str};
use tokio;
use tokio_postgres;

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    env_logger::init();
    let url: String =
        "https://api.twitter.com/2/tweets/search/stream?expansions=author_id".to_string();

    log::info!("Starting Stream From URL: {:?}", &url);
    let auth_token = match env::var("BEARER_TOKEN") {
        Ok(auth_key) => auth_key,
        Err(_) => panic!("AUTH KEY NOT FOUND!"),
    };
    log::info!("Authentication set");
    let client = reqwest::Client::builder()
        .user_agent(String::from("v2UserLookupPython"))
        .build()
        .unwrap();

    let pg_connection_url: String = get_connection_vars();
    let pg_manager = match PostgresConnectionManager::new_from_stringlike(
        &pg_connection_url,
        tokio_postgres::NoTls,
    ) {
        Ok(manager)=> manager,
        Err(e) => panic!("Unable to establish PG Manager! {:?}",e)
    };

    log::info!("Postgres Manager Created");
    let pool = match Pool::builder().build(pg_manager).await {
        Ok(pool) => pool,
        Err(e) => panic!("bb8 error! {}", e),
    };

    log::info!("Postgres Pool Created");
    let initial_conn = match pool.get().await {
        Ok(conn) => conn,
        Err(e) => panic!("Could not establish initial connection! {}", e),
    };

    let author_id_vec = match initial_conn
        .query("SELECT user_id,user_name FROM id_name_mapping;", &[])
        .await
    {
        Ok(vec) => vec,
        Err(e) => panic!("Failed to get author id mapping! {}", e),
    };

    let mut author_id_map = HashMap::<i64, String>::new();

    log::info!("Author ID Map created");
    for row in author_id_vec {
        author_id_map.insert(row.get("user_id"), row.get("user_name"));
    }

    let mut response = match client.get(url).bearer_auth(auth_token).send().await {
        Ok(response) => response,
        Err(_) => panic!("AUTH NOT SET!"),
    };

    while let chunk = response.chunk().await {

        let pool = pool.clone();
        let author_id_map = author_id_map.clone();

        log::debug!("Chunk: {:?}", chunk);

        match chunk {
            Ok(Some(byte)) => tokio::task::spawn(async move {
                parse_bytes(byte, pool, author_id_map).await;
            }), 
            Ok(None) => panic!("Something went wrong in the API Stream! Optional was NONE"),
            Err(e) => panic!("Something went wrong in the API Stream! UNKNOWN ERROR {}",e),
        };
    }
    log::warn!("Loop broken, attempting to re-run main()");
    main();
    panic!("LOOP BROKEN! COULD NOT RE-START LOOP!")
}


fn get_connection_vars() -> String {
    // let url : String = String::from("redis://localhost:6379");

    let host = match env::var("POSTGRES_HOST") {
        Ok(host_string) => host_string,
        Err(_) => String::from("HOST_NOT_SET"),
    };

    let dbname = match env::var("POSTGRES_DBNAME") {
        Ok(host_string) => host_string,
        Err(_) => String::from("DBNAME_NOT_SET"),
    };

    let postgres_password = match env::var("POSTGRES_PASS") {
        Ok(host_string) => host_string,
        Err(_) => String::from("PASSWORD_NOT_SET"),
    };

    let postgres_user = match env::var("POSTGRES_USER") {
        Ok(host_string) => host_string,
        Err(_) => String::from("postgres")
    };

    let postgres_port = match env::var("POSTGRES_PORT") {
        Ok(host_string) => host_string,
        Err(_) => String::from("5432")
    };

    let postgres_url: String = format!(
        "host={} user={} dbname={} password={} port={}",
        host, postgres_user, dbname, postgres_password, postgres_port
    );

    postgres_url
}

#[derive(Serialize, Deserialize, Debug)]
struct Tweet {
    author_id: i64,
    tweet_id: i64,
    text: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RuleResponse {
    data: serde_json::Value,
    includes: serde_json::Value,
}

fn parse_rule_response(rr: RuleResponse) -> Tweet {
    // remove all of these unwrappers
    Tweet {
        author_id: rr.data["author_id"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
        tweet_id: rr.data["id"].as_str().unwrap().parse::<i64>().unwrap(),
        text: rr.data["text"].to_string(),
    }
}

async fn parse_bytes(
    chunk: Bytes,
    sql_pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>>,
    author_id_mapping: HashMap<i64, String>,
) {
    log::debug!("Parsing the following string: {:?}", chunk);
    let heartbeat_response: Bytes = Bytes::from_static(b"\r\n");
    if chunk == heartbeat_response {
       log::info!("Heartbeat Response");
    } else {
        let json: RuleResponse = match serde_json::from_slice(&chunk) {
            Ok(json) => json,
            Err(e) => panic!("something went wrong in tranlasting the JSON: {}", e), // Make this not a panic
        };
        let tweet = parse_rule_response(json);
        // println!("{:?}",tweet)
        let _final_message = tokio::task::spawn(async move {
            postgres_insert(tweet, sql_pool, author_id_mapping)
                .await
                .unwrap();
        });
    }
}

async fn postgres_insert(
    tweet: Tweet,
    sql_pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>>,
    author_id_mapping: HashMap<i64, String>,
) -> Result<(), tokio_postgres::Error> {
    let author_id: i64 = tweet.author_id;
    log::debug!("Searching for Author ID {}",author_id);
    // let author_name: String = author_id_mapping[&author_id].to_string();
    let author_name: String = match author_id_mapping.get(&author_id) { // add code to automatically update authors that are not in the hashmap
        Some(author_name) => author_name.to_string(),
        None => panic!("Could not find author name in mapping! {}", author_id)
    };
    let tweet_id: i64 = tweet.tweet_id;
    let tweet_text: String = tweet.text;

    let sql_conn = sql_pool.get().await.unwrap();
    let mod_row_count = match sql_conn.execute("INSERT INTO TWEETS (tweet_id, author_id, author_name, tweet_text) VALUES ($1, $2, $3, $4);", &[&tweet_id, &author_id,&author_name, &tweet_text]).await {
        Ok(row_count) => row_count,
        Err(e) => panic!("Error when adding value to SQL: {}", e)
    };
    log::info!("Statement for id {} executed! rows modified: {}",tweet_id, mod_row_count);
    log::info!("Adding to ledger");
    add_to_ledger(String::from("set"), tweet_id.to_string(), author_id.to_string(), author_name.to_string(), tweet_text.to_string());
    Ok(())
    
}
