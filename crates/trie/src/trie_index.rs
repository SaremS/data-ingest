use arc_swap::ArcSwap;
use std::sync::Arc;

use crate::{
    hierarchical_index::{HierarchicalIndex, HierarchicalTopic}, 
    trie_index_internal::TrieIndexInternal
};

pub struct TrieIndex<T: Clone, const VEC_CAP: usize, const STR_CAP: usize> {
    internal: ArcSwap<TrieIndexInternal<T, VEC_CAP, STR_CAP>>,
}

impl<T: Clone, const VEC_CAP: usize, const STR_CAP: usize> TrieIndex<T, VEC_CAP, STR_CAP> {
    pub fn new() -> Self {
        TrieIndex {
            internal: ArcSwap::from_pointee(TrieIndexInternal::new()),
        }
    }

    pub fn insert_and_set_at_index(&self, topic: HierarchicalIndex<VEC_CAP, STR_CAP>, value: T) {
        self.internal.rcu(|current_arc| {
            let mut new_arc = Arc::clone(current_arc);
            let inner_mut = Arc::make_mut(&mut new_arc);
            inner_mut.insert_and_set_at_index(topic.clone(), value.clone());
            new_arc
        });
    }

    pub fn get_at_index(&self, topic: HierarchicalTopic<VEC_CAP, STR_CAP>) -> Vec<T> {
        let internal = self.internal.load();
        internal.get_at_index(topic)
    }
}


#[cfg(test)]
mod async_tests {
    use super::*;
    use std::sync::Arc;

    const VEC_CAP: usize = 5;
    const STR_CAP: usize = 10;

    #[tokio::test]
    async fn test_async_basic_insert_and_get() {
        let trie = TrieIndex::<String, VEC_CAP, STR_CAP>::new();

        trie.insert_and_set_at_index(
            HierarchicalIndex::<VEC_CAP, STR_CAP>::from_str("a.b.c").unwrap(),
            "async_value".to_string(),
        );

        let result = trie.get_at_index(
            HierarchicalTopic::<VEC_CAP, STR_CAP>::from_str("a.b.c").unwrap(),
        );

        assert_eq!(result, vec!["async_value".to_string()]);
    }

    #[tokio::test]
    async fn test_concurrent_inserts_prevent_lost_updates() {
        let trie = Arc::new(TrieIndex::<String, VEC_CAP, STR_CAP>::new());
        let mut handles = Vec::new();

        for i in 0..50 {
            let trie_clone = Arc::clone(&trie);
            let handle = tokio::spawn(async move {
                let topic_str = format!("t.{}", i);
                let insert_topic = HierarchicalIndex::<VEC_CAP, STR_CAP>::from_str(&topic_str).unwrap();
                
                trie_clone.insert_and_set_at_index(insert_topic, format!("val_{}", i));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Task panicked");
        }

        for i in 0..50 {
            let topic_str = format!("t.{}", i);
            let search_topic = HierarchicalTopic::<VEC_CAP, STR_CAP>::from_str(&topic_str).unwrap();
            
            let results = trie.get_at_index(search_topic);
            
            assert_eq!(results.len(), 1, "Missing value for topic: {}", topic_str);
            assert_eq!(results[0], format!("val_{}", i));
        }
    }

    #[tokio::test]
    async fn test_concurrent_reads_during_writes() {
        let trie = Arc::new(TrieIndex::<String, VEC_CAP, STR_CAP>::new());

        trie.insert_and_set_at_index(
            HierarchicalIndex::<VEC_CAP, STR_CAP>::from_str("a.*.c").unwrap(),
            "seed_value".to_string(),
        );

        let mut write_handles = Vec::new();
        let mut read_handles = Vec::new();

        for i in 0..50 {
            let trie_clone = Arc::clone(&trie);
            write_handles.push(tokio::spawn(async move {
                let topic_str = format!("w.{}", i);
                let insert_topic = HierarchicalIndex::<VEC_CAP, STR_CAP>::from_str(&topic_str).unwrap();
                trie_clone.insert_and_set_at_index(insert_topic, format!("val_{}", i));
            }));
        }

        for _ in 0..50 {
            let trie_clone = Arc::clone(&trie);
            read_handles.push(tokio::spawn(async move {
                let search_topic = HierarchicalTopic::<VEC_CAP, STR_CAP>::from_str("a.b.c").unwrap();
                let results = trie_clone.get_at_index(search_topic);
                
                assert!(
                    results.contains(&"seed_value".to_string()),
                    "Reader failed to see seed value during concurrent writes"
                );
            }));
        }

        for handle in write_handles {
            handle.await.expect("Writer panicked");
        }
        for handle in read_handles {
            handle.await.expect("Reader panicked");
        }
    }
}
