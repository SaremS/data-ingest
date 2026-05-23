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

    pub fn parts_as_strings(&self) -> Vec<String> {
        self.topic_parts
            .iter()
            .map(|part| part.as_str().to_string())
            .collect()
    }
}

impl<const VEC_CAP: usize, const STR_CAP: usize> Debug for HierarchicalTopic<VEC_CAP, STR_CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HierarchicalTopic({})", self.to_string())
    }
}

impl<const VEC_CAP: usize, const STR_CAP: usize> From<HierarchicalTopic<VEC_CAP, STR_CAP>>
    for String
{
    fn from(val: HierarchicalTopic<VEC_CAP, STR_CAP>) -> Self {
        val.to_string()
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub enum IndexLevel<const CAP: usize> {
    String(ArrayString<CAP>),
    Wildcard,
}

#[derive(Error, Debug)]
pub enum IndexLevelError {
    #[error("Capacity exceeded for part")]
    PartCapacityExceeded,
}

impl<const CAP: usize> IndexLevel<CAP> {
    fn from_str(s: &str) -> Result<Self, IndexLevelError> {
        if s == "*" {
            return Ok(IndexLevel::Wildcard);
        }

        let array_str =
            ArrayString::<CAP>::from(s).map_err(|_| IndexLevelError::PartCapacityExceeded)?;
        Ok(IndexLevel::String(array_str))
    }
}

impl<const CAP: usize> Debug for IndexLevel<CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexLevel::String(s) => write!(f, "{}", s),
            IndexLevel::Wildcard => write!(f, "*"),
        }
    }
}

impl<const CAP: usize> From<IndexLevel<CAP>> for String {
    fn from(val: IndexLevel<CAP>) -> Self {
        match val {
            IndexLevel::String(s) => s.as_str().to_string(),
            IndexLevel::Wildcard => "*".to_string(),
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct HierarchicalIndex<const VEC_CAP: usize, const STR_CAP: usize> {
    index_parts: ArrayVec<IndexLevel<STR_CAP>, VEC_CAP>,
}

#[derive(Error, Debug)]
pub enum HierarchicalIndexError {
    #[error("Maximum index depth exceeded")]
    MaxDepthExceeded,

    #[error("Capacity exceeded for part")]
    PartCapacityExceeded,

    #[error("Invalid format: {0}")]
    InvalidFormat(Cow<'static, str>),
}

impl<const VEC_CAP: usize, const STR_CAP: usize> HierarchicalIndex<VEC_CAP, STR_CAP> {
    pub fn new(root: &str) -> Result<Self, HierarchicalIndexError> {
        if !Self::is_valid_part(root) {
            return Err(HierarchicalIndexError::InvalidFormat(
                "Root index must be non-empty, alphanumeric or wildcard".into(),
            ));
        }
        let mut index_parts = ArrayVec::new();

        let root = IndexLevel::<STR_CAP>::from_str(root)
            .map_err(|_| HierarchicalIndexError::PartCapacityExceeded)?;

        index_parts.push(root);
        Ok(Self { index_parts })
    }

    fn is_valid_part(part: &str) -> bool {
        //must be either wildcard or alphanumeric string
        if part == "*" {
            return true;
        }
        if part.chars().all(|c| c.is_alphanumeric()) {
            return true;
        }
        false
    }

    pub fn matches_topic(&self, topic: &HierarchicalTopic<VEC_CAP, STR_CAP>) -> bool {
        let index_len = self.index_parts.len();

        for i in 0..index_len {
            let index_part = &self.index_parts[i];
            let topic_part = &topic.topic_parts[i];

            match index_part {
                IndexLevel::String(s) => {
                    if s.as_str() != topic_part.as_str() {
                        return false;
                    }
                }
                IndexLevel::Wildcard => continue,
            }
        }
        true
    }

    pub fn add_part(&mut self, part: &str) -> Result<(), HierarchicalIndexError> {
        if self.index_parts.len() >= VEC_CAP {
            return Err(HierarchicalIndexError::MaxDepthExceeded);
        }
        if !Self::is_valid_part(part) {
            return Err(HierarchicalIndexError::InvalidFormat(
                "Index part must be non-empty, alphanumeric or wildcard".into(),
            ));
        }

        let level = IndexLevel::<STR_CAP>::from_str(part)
            .map_err(|_| HierarchicalIndexError::PartCapacityExceeded)?;
        self.index_parts.push(level);
        Ok(())
    }

    pub fn from_str(index_str: &str) -> Result<Self, HierarchicalIndexError> {
        let parts: Vec<&str> = index_str.split('.').collect();

        if parts.is_empty() {
            return Err(HierarchicalIndexError::InvalidFormat(
                "Index string cannot be empty".into(),
            ));
        }
        if parts.len() > VEC_CAP {
            return Err(HierarchicalIndexError::MaxDepthExceeded);
        }

        let mut index_parts = ArrayVec::new();
        for part in parts {
            if !Self::is_valid_part(part) {
                return Err(HierarchicalIndexError::InvalidFormat(
                    "Index part must be non-empty, alphanumeric or wildcard".into(),
                ));
            }
            let level = IndexLevel::<STR_CAP>::from_str(part)
                .map_err(|_| HierarchicalIndexError::PartCapacityExceeded)?;
            index_parts.push(level);
        }
        Ok(Self { index_parts })
    }

    pub fn is_empty(&self) -> bool {
        self.index_parts.is_empty()
    }

    pub fn to_string(&self) -> String {
        self.index_parts
            .iter()
            .map(|part| match part {
                IndexLevel::String(s) => s.as_str(),
                IndexLevel::Wildcard => "*",
            })
            .collect::<Vec<&str>>()
            .join(".")
    }

    pub fn parts_as_strings(&self) -> Vec<String> {
        self.index_parts
            .iter()
            .map(|part| match part {
                IndexLevel::String(s) => s.as_str().to_string(),
                IndexLevel::Wildcard => "*".to_string(),
            })
            .collect()
    }
}

impl<const VEC_CAP: usize, const STR_CAP: usize> Debug for HierarchicalIndex<VEC_CAP, STR_CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HierarchicalIndex({})", self.to_string())
    }
}

impl<const VEC_CAP: usize, const STR_CAP: usize> From<HierarchicalIndex<VEC_CAP, STR_CAP>>
    for String
{
    fn from(val: HierarchicalIndex<VEC_CAP, STR_CAP>) -> Self {
        val.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_level_from_str() {
        let level = IndexLevel::<12>::from_str("test").unwrap();
        assert!(matches!(level, IndexLevel::String(s) if s.as_str() == "test"));

        let wildcard = IndexLevel::<12>::from_str("*").unwrap();
        assert!(matches!(wildcard, IndexLevel::Wildcard));

        assert!(matches!(
            IndexLevel::<12>::from_str("thisisaverylongpart"),
            Err(IndexLevelError::PartCapacityExceeded)
        ));
    }

    #[test]
    fn test_hierarchical_index_creation() {
        let index = HierarchicalIndex::<3, 12>::new("root").unwrap();
        assert_eq!(index.to_string(), "root");
    }

    #[test]
    fn test_hierarchical_index_add_part() {
        let mut index = HierarchicalIndex::<3, 12>::new("root").unwrap();
        index.add_part("child").unwrap();
        assert_eq!(index.to_string(), "root.child");
    }

    #[test]
    fn test_hierarchical_index_from_str() {
        let index = HierarchicalIndex::<3, 12>::from_str("root.child").unwrap();
        assert_eq!(index.to_string(), "root.child");
    }

    #[test]
    fn test_hierarchical_index_errors() {
        assert!(matches!(
            HierarchicalIndex::<3, 12>::new("thisisaverylongroot"),
            Err(HierarchicalIndexError::PartCapacityExceeded)
        ));

        let mut index = HierarchicalIndex::<3, 12>::new("root").unwrap();
        assert!(matches!(
            index.add_part("thisisaverylongchild"),
            Err(HierarchicalIndexError::PartCapacityExceeded)
        ));

        assert!(matches!(
            HierarchicalIndex::<3, 12>::from_str("part1.part2.part3.part4"),
            Err(HierarchicalIndexError::MaxDepthExceeded)
        ));

        let mut index = HierarchicalIndex::<3, 12>::new("root").unwrap();
        assert!(matches!(index.add_part("child1"), Ok(())));
        assert!(matches!(index.add_part("child2"), Ok(())));
        assert!(matches!(
            index.add_part("child3"),
            Err(HierarchicalIndexError::MaxDepthExceeded)
        ));
    }

    #[test]
    fn test_hierarchical_index_partial_eq() {
        let index1 = HierarchicalIndex::<3, 12>::from_str("root.child.a").unwrap();
        let index2 = HierarchicalIndex::<3, 12>::from_str("root.child.a").unwrap();
        let index3 = HierarchicalIndex::<3, 12>::from_str("root.*.a").unwrap();

        assert_eq!(index1, index2);
        assert_ne!(index1, index3);
    }

    #[test]
    fn test_hierarchical_index_matches_topic() {
        let index = HierarchicalIndex::<3, 12>::from_str("root.*.a").unwrap();
        let topic1 = HierarchicalTopic::<3, 12>::from_str("root.child.a").unwrap();
        let topic2 = HierarchicalTopic::<3, 12>::from_str("root.child.b").unwrap();
        let topic3 = HierarchicalTopic::<3, 12>::from_str("root.child.extra").unwrap();

        assert!(index.matches_topic(&topic1));
        assert!(!index.matches_topic(&topic2));
        assert!(!index.matches_topic(&topic3));
    }
}
