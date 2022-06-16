use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use bytes::Bytes;
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
    let auth_token = match env::var("BEARER_AUTH") {
        Ok(auth_key) => auth_key,
        Err(_) => panic!("AUTH KEY NOT FOUND!"),
    };
    log::info!("Authentication set");
    let client = reqwest::Client::builder()
        .user_agent(String::from("v2UserLookupPython"))
        .build()
        .unwrap();

    let pg_manager = match PostgresConnectionManager::new_from_stringlike(
        "host=localhost user=sami dbname=tweet_audit password=admin port=5432",
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
        .query("SELECT DISTINCT author_id, author_name FROM tweets;", &[])
        .await
    {
        Ok(vec) => vec,
        Err(e) => panic!("Failed to get author id mapping! {}", e),
    };

    let mut author_id_map = HashMap::<i64, String>::new();

    log::info!("Author ID Map created");
    for row in author_id_vec {
        author_id_map.insert(row.get("author_id"), row.get("author_name"));
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
            Err(_) => panic!("Something went wrong in the API Stream! UNKNOWN ERROR"),
        };
    }

    panic!("LOOP BROKEN!")
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
    let author_name: String = author_id_mapping[&author_id].to_string();
    let tweet_id: i64 = tweet.tweet_id;
    let tweet_text: String = tweet.text;

    let sql_conn = sql_pool.get().await.unwrap();
    let mod_row_count = match sql_conn.execute("INSERT INTO TWEETS (tweet_id, author_id, author_name, tweet_text) VALUES ($1, $2, $3, $4);", &[&tweet_id, &author_id,&author_name, &tweet_text]).await {
        Ok(row_count) => row_count,
        Err(e) => panic!("Error when adding value to SQL: {}", e)
    };
    log::info!("Statement for id {} executed! rows modified: {}",tweet_id, mod_row_count);
    Ok(())
}
