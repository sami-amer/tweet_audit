extern crate openssl;
extern crate protobuf;
extern crate rand;
extern crate serde;
extern crate serde_cbor;

use serde::{Deserialize, Serialize};

use protobuf::Message;

use crate::transaction_utils::create_batch_list;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TwitPayload {
    verb: String,
    tweet_id: String,
    author_id: String,
    author_name: String,
    tweet_text: String,
}

pub fn add_to_ledger(verb: String, tweet_id: String, author_id: String, author_name: String, tweet_text: String) {
    // let payload = Payload {verb: String::from("set"), name: String::from("foo"), value: 42};
    let payload = TwitPayload {
        verb, 
        tweet_id,
        author_id,
        author_name,
        tweet_text
    };
    let payload_bytes = serde_cbor::to_vec(&payload).expect("upsi");
    let batch_list = create_batch_list(payload_bytes);

    let batch_list_bytes = batch_list
        .write_to_bytes()
        .expect("Error converting batch list to bytes");

    // * Sending Batch List to Validator from within rust
    extern crate reqwest;

    // ! should probably make this async
    let client = reqwest::blocking::Client::new();
    let res = client
        .post("http://localhost:8008/batches")
        .header("Content-Type", "application/octet-stream")
        .body(batch_list_bytes)
        .send();

    println!("{:?}", res.unwrap());

    // // * Creating Batch List to send manually
    // use std::fs::File;
    // use std::io::Write;

    // let mut file = File::create("tweets.batches").expect("Error creating files");
    // file.write_all(&batch_list_bytes)
    //     .expect("Error writing bytes");
}

pub fn to_hex_string(bytes: &Vec<u8>) -> String {
    let strs: Vec<String> = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    strs.join("")
}
