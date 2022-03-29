use postgres::{Client, NoTls};
use redis::{self, Commands};
use serde::{Deserialize, Serialize};
use std::env;
fn main() {
    println!("HELLO RUST!");

    let redis_url = match env::var("REDIS_URL") {
        Ok(host_string) => host_string,
        Err(_) => String::from("REDIS_URL_NOT_SET"),
    };
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

    let postgres_url: String = format!(
        "host={} user=postgres dbname={} password = {} port=5432",
        host, dbname, postgres_password
    );
    println!("URL is {}", &postgres_url);
    loop {
        // let mut client = Client::connect("host=localhost user=postgres dbname=rust_test", NoTls).unwrap();
        let mut client = match Client::connect(&postgres_url, NoTls) {
            Ok(client) => client,
            Err(error) => panic!("Client Creation Failed! {}", error),
        };

        let response = do_redis_code(&redis_url);
        let parsed = parse_json(response.unwrap());
        let tweet = create_tweet(parsed);
        println!("Inserting Tweet with id: {}", tweet.tweet_id);
        let insert_result = match insert_values(client, tweet) {
            Ok(_) => continue,
            Err(error) => println!("Error! {}", error),
        };
    }

    println!("Loop BROKEN!")
}

fn get_ids(mut client: Client) {
    for row in client.query("SELECT tweet_id FROM tweets", &[]).unwrap() {
        let id: i64 = row.get(0);

        println!("found person: {}", id);
    }
}

fn insert_values(mut client: Client, tweet: Tweet) -> Result<(), postgres::Error> {
    client.execute(
        "INSERT INTO tweets (tweet_id, author_id, author_name, tweet_text) VALUES ($1, $2, $3, $4)",
        &[&tweet.tweet_id, &tweet.author_id, &"null",&tweet.tweet_text],
    ).unwrap();

    client.close()
}

// #[derive(Serialize, Deserialize)]
struct Tweet {
    author_id: i64,
    // author_name:String, ! Add this later, but on the python end
    tweet_id: i64,
    tweet_text: String,
}

fn parse_json(input: String) -> serde_json::Value {
    let json: serde_json::Value =
        serde_json::from_str(&input).expect("JSON was not well-formatted");
    json
}

fn create_tweet(parsed_json: serde_json::Value) -> Tweet {
    let author_id: i64 = match parsed_json["data"]["author_id"].as_str() {
        Some(id_string) => id_string.parse().unwrap(),
        // None => println!("ERROR PARSING AUTHOR ID")
        None => 0,
    };

    let tweet_id: i64 = match parsed_json["data"]["id"].as_str() {
        Some(id_string) => id_string.parse().unwrap(),
        // None => println!("ERROR PARSING TWEET ID")
        None => 0,
    };

    // let author_name: u64 = match parsed_json["data"]["author_id"].as_str(){
    //     Some(id_string) => id_string.parse().unwrap(),
    //     None => println!("ERROR")
    // };

    let tweet_text: String = match parsed_json["data"]["text"].as_str() {
        Some(text) => text.to_string(),
        // None => println!("ERROR PARSING TWEET TEXT")
        // ! Make these in to proper errors !
        None => String::from("None"),
    };

    let tweet = Tweet {
        author_id,
        tweet_id,
        tweet_text,
    };

    tweet
}

// fn do_redis_code(url: &str) -> redis::RedisResult<(String)> {
fn do_redis_code(url: &str) -> redis::RedisResult<String> {
    // general connection handling
    let client = redis::Client::open(url)?;
    let mut con = client.get_connection()?;
    let list_name = "tweets";
    // let args = String::from("tweets").ToRedisArgs();
    // let popped: redis::RedisResult<()> = con.rpop();

    println!("blocking...");
    let item: Vec<String> = con
        .brpop(list_name, 0)
        .expect("failed to execute brpop for 'tweets'");

    // let extracted: String = match redis::from_redis_value(&item){
    //     Ok(json_string) => json_string,
    //     Err(_) => String::from("ERROR/ERROR")
    //     // ! add a match in other funciton to check for this string
    // };

    let error_str = String::from("ERROR!");

    let extracted = match item.get(1) {
        Some(val) => val,
        None => &error_str,
    };
    // println!("popped item: {}", extracted);

    Ok(extracted.to_string())
}
