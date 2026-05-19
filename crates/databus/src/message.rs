use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageType {
    Data,
    Error,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    pub message_type: MessageType,
    pub message_meta: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<T: Clone + Send + Sync> {
    pub header: MessageHeader,
    pub payload: T,
}

pub struct MessageBuilder<T: Clone + Send + Sync> {
    message_type: MessageType,
    message_meta: HashMap<String, String>,
    payload: T,
}

impl<T: Clone + Send + Sync> MessageBuilder<T> {
    fn new(payload: T, message_type: MessageType) -> Self {
        Self {
            message_type,
            message_meta: HashMap::new(),
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

    pub fn new_data(payload: T) -> Self {
        Self::new(payload, MessageType::Data)
    }

    pub fn new_empty(payload: T) -> Self {
        Self::new(payload, MessageType::Empty)
    }

    pub fn new_error(payload: T) -> Self {
        Self::new(payload, MessageType::Error)
    }

    pub fn add_meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.message_meta.insert(key.into(), value.into());
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
