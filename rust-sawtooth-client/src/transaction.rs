use cbor;

use sha2::{Digest, Sha512};

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;

use cbor::encoder::GenericEncoder;
use cbor::value::Key;
use cbor::value::Text;
use cbor::value::Value;

use sawtooth_sdk::messages::processor::TpProcessRequest;
use sawtooth_sdk::processor::handler::ApplyError;
use sawtooth_sdk::processor::handler::TransactionContext;
use sawtooth_sdk::processor::handler::TransactionHandler;

use log::info;
// use bytes::Bytes;
// use serde;
// use serde_json;
// use serde_cbor;

fn get_twit_prefix() -> String {
    hex::encode(Sha512::digest(b"twit"))[..6].to_string()
}

#[derive(Clone, Copy)]
enum Verb {
    Set,
    Delete,
    Get,
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Verb::Set => "Verb::Set",
                Verb::Delete => "Verb::Delete",
                Verb::Get => "Verb::Return",
            }
        )
    }
}
struct TwitPayload { 
    verb: Verb,
    name: String,
    // value: serde_json::Value,
    value: u32 
}

impl TwitPayload {
    pub fn new(payload_data: &[u8]) -> Result<Option<TwitPayload>, ApplyError> {
        let input = Cursor::new(payload_data);

        let mut decoder = cbor::GenericDecoder::new(cbor::Config::default(), input);
        let decoder_value = decoder
            .value()
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;

        let c = cbor::value::Cursor::new(&decoder_value);

        let verb_raw: String = match c.field("Verb").text_plain() {
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Verb must be 'set', 'del', or 'return'",
                )));
            }
            Some(verb_raw) => verb_raw.clone(),
        };

        let verb = match verb_raw.as_str() {
            "set" => Verb::Set,
            "del" => Verb::Delete,
            "get" => Verb::Get,
            _ => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Verb must be 'set', 'inc' or 'dec'",
                )));
            }
        };

        let value_raw = c.field("Value");
        let value_raw = match value_raw.value() {
            Some(x) => x,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Must have a value",
                )));
            }
        };
        
        let value: u32 = match *value_raw {
            cbor::value::Value::U8(x) => u32::from(x),
            cbor::value::Value::U16(x) => u32::from(x),
            cbor::value::Value::U32(x) => x,
            _ => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Value must be an integer",
                )));
            }
        };

        let name_raw: String = match c.field("Name").text_plain() {
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Name must be a string",
                )));
            }
            Some(name_raw) => name_raw.clone(),
        };

        let twit_payload = TwitPayload {
            verb,
            name: name_raw,
            value
        };
        Ok(Some(twit_payload))
    }

    pub fn get_verb(&self) -> Verb {
        self.verb
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_value(&self) -> u32 {
        self.value
    }
}

pub struct TwitState<'a> {
    context: &'a mut dyn TransactionContext,
    get_cache: HashMap<String, BTreeMap<Key, Value>>,
}

impl<'a> TwitState<'a> {
    pub fn new(context: &'a mut dyn TransactionContext) -> TwitState {
        TwitState {
            context,
            get_cache: HashMap::new(),
        }
    }

    fn calculate_address(name: &str) -> String {
        let sha = hex::encode(Sha512::digest(name.as_bytes()))[64..].to_string();
        get_twit_prefix() + &sha
    }

    pub fn get(&mut self, name: &str) -> Result<Option<u32>, ApplyError> {
        let address = TwitState::calculate_address(name);
        let d = self.context.get_state_entry(&address)?;
        match d {
            Some(packed) => {
                let input = Cursor::new(packed);
                let mut decoder = cbor::GenericDecoder::new(cbor::Config::default(), input);
                let map_value = decoder
                    .value()
                    .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;
                let map = match map_value {
                    Value::Map(m) => m,
                    _ => {
                        return Err(ApplyError::InternalError(String::from(
                            "No map returned from state",
                        )));
                    }
                };

                let status = match map.get(&Key::Text(Text::Text(String::from(name)))) {
                    Some(v) => match *v {
                        Value::U32(x) => Ok(Some(x)),
                        Value::U16(x) => Ok(Some(u32::from(x))),
                        Value::U8(x) => Ok(Some(u32::from(x))),
                        _ => Err(ApplyError::InternalError(String::from(
                            "Value returned from state is the wrong type.",
                        ))),
                    },
                    None => Ok(None),
                };
                self.get_cache.insert(address, map);
                status
            }
            None => Ok(None),
        }
    }

    pub fn set(&mut self, name: &str, value: u32) -> Result<(), ApplyError> {
        let mut map: BTreeMap<Key, Value> = match self
            .get_cache
            .get_mut(&TwitState::calculate_address(name))
        {
            Some(m) => m.clone(),
            None => BTreeMap::new(),
        };
        map.insert(Key::Text(Text::Text(String::from(name))), Value::U32(value));

        let mut e = GenericEncoder::new(Cursor::new(Vec::new()));
        e.value(&Value::Map(map))
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;

        let packed = e.into_inner().into_writer().into_inner();
        self.context
            .set_state_entry(TwitState::calculate_address(name), packed)
            .map_err(|err| ApplyError::InternalError(format!("{}", err)))?;

        Ok(())
    }
}

pub struct TwitTransactionHandler {
    family_name: String,
    family_versions: Vec<String>,
    namespaces: Vec<String>,
}

impl Default for TwitTransactionHandler {
    fn default() -> Self {
        TwitTransactionHandler {
            family_name: "twit".to_string(),
            family_versions: vec!["2.0".to_string()],
            namespaces: vec![get_twit_prefix()],
        }
    }
}

impl TwitTransactionHandler {
    pub fn new() -> TwitTransactionHandler {
        Self::default()
    }
}

impl TransactionHandler for TwitTransactionHandler {
    fn family_name(&self) -> String {
        self.family_name.clone()
    }

    fn family_versions(&self) -> Vec<String> {
        self.family_versions.clone()
    }

    fn namespaces(&self) -> Vec<String> {
        self.namespaces.clone()
    }

    fn apply(
        &self,
        request: &TpProcessRequest,
        context: &mut dyn TransactionContext,
    ) -> Result<(), ApplyError> {
        let payload = TwitPayload::new(request.get_payload());
        let payload = match payload {
            Err(e) => return Err(e),
            Ok(payload) => payload,
        };
        let payload = match payload {
            Some(x) => x,
            None => {
                return Err(ApplyError::InvalidTransaction(String::from(
                    "Request must contain a payload",
                )));
            }
        };

        let mut state = TwitState::new(context);

        info!(
            "payload: {} {} {} {} {}",
            payload.get_verb(),
            payload.get_name(),
            payload.get_value(),
            request.get_header().get_inputs()[0],
            request.get_header().get_outputs()[0]
        );

        match payload.get_verb() {
            Verb::Set => {
                match state.get(payload.get_name()) {
                    Ok(Some(_)) => {
                        return Err(ApplyError::InvalidTransaction(format!(
                            "{} already set",
                            payload.get_name()
                        )));
                    }
                    Ok(None) => (),
                    Err(err) => return Err(err),
                };
                state.set(payload.get_name(), payload.get_value())
            }
            Verb::Delete => {
                
                state.set(payload.get_name(), 0) 
            },

            Verb::Get => {
                
                state.set(payload.get_name(), 99 ) 
            },
            }
        }
    }

