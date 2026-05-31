use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;

use arrayvec::ArrayString;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessageError {
    #[error("Invalid topic: {0}")]
    CreationError(Cow<'static, str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageType {
    Data,
    Error,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    pub message_type: MessageType,
    pub message_meta: Option<HashMap<ArrayString<16>, ArrayString<24>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<T: Clone + Send + Sync> {
    pub header: MessageHeader,
    pub payload: T,
}

pub struct MessageBuilder<T: Clone + Send + Sync> {
    message_type: MessageType,
    message_meta: Option<HashMap<ArrayString<16>, ArrayString<24>>>,
    payload: T,
}

impl<T: Clone + Send + Sync> MessageBuilder<T> {
    fn new(payload: T, message_type: MessageType) -> Result<Self, MessageError> {
        Ok(Self {
            message_type,
            message_meta: None,
            payload,
        })
    }

    pub fn new_from_topic(payload: T, message_type: MessageType) -> Self {
        Self {
            message_type,
            message_meta: None,
            payload,
        }
    }

    pub fn new_from_message(message: Message<T>) -> Self {
        Self {
            message_type: message.header.message_type,
            message_meta: message.header.message_meta,
            payload: message.payload,
        }
    }

    pub fn new_data(payload: T) -> Result<Self, MessageError> {
        Self::new(payload, MessageType::Data)
    }

    pub fn new_data_from_topic(payload: T) -> Self {
        Self::new_from_topic(payload, MessageType::Data)
    }

    pub fn new_empty(payload: T) -> Result<Self, MessageError> {
        Self::new(payload, MessageType::Empty)
    }

    pub fn new_error(payload: T) -> Result<Self, MessageError> {
        Self::new(payload, MessageType::Error)
    }

    pub fn add_meta(mut self, key: &str, value: &str) -> Self {
        //shorten the key and value to fit into ArrayString<32>
        let key = if key.len() > 16 {
            ArrayString::from(&key[..16]).unwrap()
        } else {
            ArrayString::from(key).unwrap()
        };

        let value = if value.len() > 24 {
            ArrayString::from(&value[..24]).unwrap()
        } else {
            ArrayString::from(value).unwrap()
        };

        if let Some(ref mut meta) = self.message_meta {
            meta.insert(key, value);
        } else {
            let mut meta = HashMap::new();
            meta.insert(key, value);
            self.message_meta = Some(meta);
        }
        self
    }

    pub fn build(self) -> Message<T> {
        Message {
            header: MessageHeader {
                message_type: self.message_type,
                message_meta: self.message_meta,
            },
            payload: self.payload,
        }
    }
}
