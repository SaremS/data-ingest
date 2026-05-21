use std::borrow::Cow;
use std::fmt::Debug;

use arrayvec::{ArrayString, ArrayVec};
use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HierarchicalTopic<const VEC_CAP: usize, const STR_CAP: usize> {
    topic_parts: ArrayVec<ArrayString<STR_CAP>, VEC_CAP>,
}

#[derive(Error, Debug)]
pub enum HierarchicalTopicError {
    #[error("Maximum index depth exceeded")]
    MaxDepthExceeded,

    #[error("Capacity exceeded for part")]
    PartCapacityExceeded,

    #[error("Invalid topic format: {0}")]
    InvalidFormat(Cow<'static, str>),

    #[error("Disallowed character - only letters and numbers are allowed: {0}")]
    InvalidCharacter(Cow<'static, str>),
}

impl<const VEC_CAP: usize, const STR_CAP: usize> HierarchicalTopic<VEC_CAP, STR_CAP> {
    pub fn new(root: &str) -> Result<Self, HierarchicalTopicError> {
        if !Self::is_valid_part(root) {
            return Err(HierarchicalTopicError::InvalidCharacter(
                "Root topic must be non-empty, alphanumeric or wildcard".into(),
            ));
        }

        let mut topic_parts = ArrayVec::new();

        topic_parts.push(
            ArrayString::from(root).map_err(|_| HierarchicalTopicError::PartCapacityExceeded)?,
        );
        Ok(Self { topic_parts })
    }

    fn is_valid_part(part: &str) -> bool {
        //must be alphanumeric and not empty
        if part.is_empty() {
            return false;
        }
        part.chars().all(|c| c.is_alphanumeric())
    }

    pub fn add_part(&mut self, part: &str) -> Result<(), HierarchicalTopicError> {
        if self.topic_parts.len() >= VEC_CAP {
            return Err(HierarchicalTopicError::MaxDepthExceeded);
        }
        if !Self::is_valid_part(part) {
            return Err(HierarchicalTopicError::InvalidCharacter(
                "Topic part must be non-empty and alphanumeric".into(),
            ));
        }

        let part =
            ArrayString::from(part).map_err(|_| HierarchicalTopicError::PartCapacityExceeded)?;

        self.topic_parts.push(part);
        Ok(())
    }

    pub fn from_str(topic_str: &str) -> Result<Self, HierarchicalTopicError> {
        let parts: Vec<&str> = topic_str.split('.').collect();

        if parts.is_empty() {
            return Err(HierarchicalTopicError::InvalidFormat(
                "Topic string cannot be empty".into(),
            ));
        }
        if parts.len() > VEC_CAP {
            return Err(HierarchicalTopicError::MaxDepthExceeded);
        }

        let mut topic_parts = ArrayVec::new();
        for part in parts {
            if !Self::is_valid_part(part) {
                return Err(HierarchicalTopicError::InvalidCharacter(
                    "Topic parts must be non-empty and alphanumeric".into(),
                ));
            }
            let part = ArrayString::from(part)
                .map_err(|_| HierarchicalTopicError::PartCapacityExceeded)?;
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

impl<const VEC_CAP: usize, const STR_CAP: usize> Debug for HierarchicalTopic<VEC_CAP, STR_CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HierarchicalTopic({})", self.to_string())
    }
}

impl<const VEC_CAP: usize, const STR_CAP: usize> Into<String>
    for HierarchicalTopic<VEC_CAP, STR_CAP>
{
    fn into(self) -> String {
        self.to_string()
    }
}
