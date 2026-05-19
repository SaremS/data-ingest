use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;

use arrayvec::ArrayVec;
use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HierarchicalTopic {
    topic_parts: ArrayVec<String, 5>,
}

#[derive(Error, Debug)]
pub enum HierarchicalTopicError {
    #[error("Maximum topic depth exceeded")]
    MaxDepthExceeded,

    #[error("Invalid topic format: {0}")]
    InvalidFormat(Cow<'static, str>),
}

impl HierarchicalTopic {
    pub fn new(root: impl Into<String>) -> Self {
        let mut topic_parts = ArrayVec::new();
        topic_parts.push(root.into());
        Self { topic_parts }
    }

    pub fn add_part(&mut self, part: String) -> Result<(), HierarchicalTopicError> {
        if self.topic_parts.len() >= 5 {
            return Err(HierarchicalTopicError::MaxDepthExceeded);
        }
        self.topic_parts.push(part);
        Ok(())
    }

    pub fn from_str(topic_str: &str) -> Result<Self, HierarchicalTopicError> {
        let parts: Vec<String> = topic_str
            .split('.')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            return Err(HierarchicalTopicError::InvalidFormat(
                "Topic string cannot be empty".into(),
            ));
        }
        if parts.len() > 5 {
            return Err(HierarchicalTopicError::MaxDepthExceeded);
        }

        let mut topic_parts = ArrayVec::new();
        for part in parts {
            topic_parts.push(part);
        }
        Ok(Self { topic_parts })
    }

    pub fn is_empty(&self) -> bool {
        self.topic_parts.is_empty()
    }

    pub fn to_string(&self) -> String {
        self.topic_parts.join(".")
    }
}

impl Debug for HierarchicalTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HierarchicalTopic({})", self.to_string())
    }
}

impl Into<String> for HierarchicalTopic {
    fn into(self) -> String {
        self.to_string()
    }
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
    pub message_meta: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<T: Clone + Send + Sync> {
    pub topic: HierarchicalTopic,
    pub header: MessageHeader,
    pub payload: T,
}

pub struct MessageBuilder<T: Clone + Send + Sync> {
    topic: HierarchicalTopic,
    message_type: MessageType,
    message_meta: HashMap<String, String>,
    payload: T,
}

impl<T: Clone + Send + Sync> MessageBuilder<T> {
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
