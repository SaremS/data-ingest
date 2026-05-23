use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;

use arrayvec::ArrayVec;
use thiserror::Error;

use trie::hierarchical_index::{
    HierarchicalTopic, HierarchicalTopicError
};


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
pub struct Message<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize> {
    pub topic: HierarchicalTopic<VEC_CAP, STR_CAP>,
    pub header: MessageHeader,
    pub payload: T,
}

pub struct MessageBuilder<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize> {
    topic: HierarchicalTopic<VEC_CAP, STR_CAP>,
    message_type: MessageType,
    message_meta: HashMap<String, String>,
    payload: T,
}

impl<T: Clone + Send + Sync, const VEC_CAP: usize, const STR_CAP: usize> MessageBuilder<T, VEC_CAP, STR_CAP> {
    fn new(payload: T, message_type: MessageType, topic_root: impl Into<String>) -> Self {
        Self {
            topic: HierarchicalTopic::new(topic_root),
            message_type,
            message_meta: HashMap::new(),
            payload,
        }
    }

    pub fn new_from_message(message: Message<T>) -> Self {
        Self {
            topic: message.topic,
            message_type: message.header.message_type,
            message_meta: message.header.message_meta,
            payload: message.payload,
        }
    }

    pub fn new_data(payload: T, topic_root: impl Into<String>) -> Self {
        Self::new(payload, MessageType::Data, topic_root)
    }

    pub fn new_empty(payload: T, topic_root: impl Into<String>) -> Self {
        Self::new(payload, MessageType::Empty, topic_root)
    }

    pub fn new_error(payload: T, topic_root: impl Into<String>) -> Self {
        Self::new(payload, MessageType::Error, topic_root)
    }

    pub fn add_meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.message_meta.insert(key.into(), value.into());
        self
    }

    pub fn extend_topic(mut self, part: impl Into<String>) -> Result<Self, HierarchicalTopicError> {
        self.topic.add_part(part.into())?;
        Ok(self)
    }

    pub fn build(self) -> Message<T> {
        Message {
            topic: self.topic,
            header: MessageHeader {
                message_type: self.message_type,
                message_meta: self.message_meta,
            },
            payload: self.payload,
        }
    }
}
