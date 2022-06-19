use sawtooth_sdk::messages::transaction::{TransactionHeader, Transaction};
use rand::{thread_rng,Rng};
use protobuf::{RepeatedField, Message};
use sawtooth_sdk::signing::Signer;
use sawtooth_sdk::messages::batch::{BatchHeader,BatchList};
use sawtooth_sdk::signing::{CryptoFactory,create_context};
use openssl::sha::sha512;

pub fn create_txn_header(signer: &Signer) -> TransactionHeader{

    // * Creating Transaction Header
    let mut txn_header = TransactionHeader::new();
    txn_header.set_family_name(String::from("twit"));
    txn_header.set_family_version(String::from("2.0"));


    let mut nonce = [0u8;16];
    thread_rng()
        .try_fill(&mut nonce[..])
        .expect("Error generating random nonce");
    txn_header.set_nonce(to_hex_string(&nonce.to_vec()));

    // let input_vec: Vec<String> = vec![String::from("029594d01f14ae8e74fec8552bebf40e781f2c4bc5427882b48380083c59f89429",)];
    // let output_vec: Vec<String> = vec![String::from("029594d01f14ae8e74fec8552bebf40e781f2c4bc5427882b48380083c59f89429",)];

    let input_vec: Vec<String> = vec![String::from("19c62b",)];
    let output_vec: Vec<String> = vec![String::from("19c62b",)];

    txn_header.set_inputs(RepeatedField::from_vec(input_vec));
    txn_header.set_outputs(RepeatedField::from_vec(output_vec));

    txn_header.set_signer_public_key(
        signer
            .get_public_key()
            .expect("Error retrieving Public Key")
            .as_hex(),
    );

    txn_header.set_batcher_public_key(
        signer
            .get_public_key()
            .expect("Error Retreiving public key")
            .as_hex(),
    );
    
    txn_header
}


pub fn to_hex_string(bytes: &Vec<u8>) -> String {
    let strs: Vec<String> = bytes.iter()
        .map(|b| format!("{:02x}",b))
        .collect();
    strs.join("")
}

pub fn create_batch_header(signer: &Signer, txn: Transaction) -> BatchHeader {

    let mut batch_header = BatchHeader::new();

    batch_header.set_signer_public_key(
        signer
            .get_public_key()
            .expect("Error Retrieving Public Key")
            .as_hex(),
    );

    let transaction_ids = vec![txn.clone()]
        .iter()
        .map(|trans| String::from(trans.get_header_signature()))
        .collect();
    
    batch_header.set_transaction_ids(RepeatedField::from_vec(transaction_ids));
    batch_header

}

pub fn create_batch_list(payload_bytes:Vec<u8>) -> BatchList{
    // * Creating Signer
    let context = create_context("secp256k1")
        .expect("Error creating the right context");
    let private_key = context
        .new_random_private_key()
        .expect("Erro generating a new private key");
    let crypto_factory = CryptoFactory::new(context.as_ref());
    let signer = crypto_factory.new_signer(private_key.as_ref());

    let mut txn_header = create_txn_header(&signer);

    // ! Make the whole thing a function that can be called
    // ! Maybe from command line for now?
    // let payload = Payload {verb: String::from("set"), name: String::from("foo"), value: 42};
    // let payload_bytes = serde_cbor::to_vec(&payload).expect("upsi");

    txn_header.set_payload_sha512(to_hex_string(&sha512(&payload_bytes).to_vec()));

    let txn_header_bytes = txn_header
        .write_to_bytes()
        .expect("Error conerting transaction header to bytes");

    let signature = signer
        .sign(&txn_header_bytes)
        .expect("Error signing the transactino header");

    // * Signing and Creating Transaction

    let mut txn = Transaction::new();
    txn.set_header(txn_header_bytes.to_vec());
    txn.set_header_signature(signature);
    txn.set_payload(payload_bytes);

    // * Creating Batch Header

    let batch_header = create_batch_header(&signer, txn.clone());
    let batch_header_bytes = batch_header
        .write_to_bytes()
        .expect("Error converting batch header to byets");
    
    // * Creating Batch
    use sawtooth_sdk::messages::batch::Batch;
    
    let signature = signer
        .sign(&batch_header_bytes)
        .expect("Error signing the batch header");

    let mut batch = Batch::new();

    batch.set_header(batch_header_bytes);
    batch.set_header_signature(signature);
    batch.set_transactions(RepeatedField::from_vec(vec![txn]));

    // * Encode Batch into a Batch List

    let mut batch_list = BatchList::new();
    batch_list.set_batches(RepeatedField::from_vec(vec![batch]));
    batch_list

}

