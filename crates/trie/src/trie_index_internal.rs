use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

use crate::hierarchical_index::{HierarchicalIndex, HierarchicalTopic};

#[derive(Debug, Clone)]
pub struct TrieIndexInternal<T: Clone, const VEC_CAP: usize, const STR_CAP: usize> {
    next: HashMap<String, TrieIndexInternal<T, VEC_CAP, STR_CAP>>,
    value: Option<T>,
}

impl<T: Clone, const VEC_CAP: usize, const STR_CAP: usize> Default
    for TrieIndexInternal<T, VEC_CAP, STR_CAP>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, const VEC_CAP: usize, const STR_CAP: usize> TrieIndexInternal<T, VEC_CAP, STR_CAP> {
    pub fn new() -> Self {
        TrieIndexInternal {
            next: HashMap::new(),
            value: None,
        }
    }

    fn insert_next(&mut self, value: T, mut parts: VecDeque<String>) {
        if parts.is_empty() {
            self.value = Some(value);
        } else {
            let part = parts.pop_front().unwrap();
            let next_index = self.next.entry(part).or_insert_with(TrieIndexInternal::new);
            next_index.insert_next(value, parts);
        }
    }

    pub fn insert_and_set_at_index(
        &mut self,
        topic: &HierarchicalIndex<VEC_CAP, STR_CAP>,
        value: T,
    ) {
        let parts = topic.parts_as_strings();
        let parts_deque: VecDeque<String> = VecDeque::from(parts);

        self.insert_next(value, parts_deque);
    }

    fn get_next(&self, mut parts: VecDeque<String>) -> Vec<T> {
        let mut results = Vec::new();
        let mut found = Vec::new();

        let current_part = parts.pop_front();

        if let Some(current_part) = current_part {
            let wildcard_found = self.next.get("*");
            let part_found = self.next.get(&current_part);

            found.append(&mut wildcard_found.into_iter().collect());
            found.append(&mut part_found.into_iter().collect());

            if found.is_empty() {
                if let Some(v) = &self.value {
                    results.push(v.clone())
                }
                return results;
            }

            for next_index in found {
                let mut next_results = next_index.get_next(parts.clone());
                results.append(&mut next_results);
            }
        } else if let Some(value) = &self.value {
            results.push(value.clone());
        }

        results
    }

    pub fn get_at_index(&self, topic: &HierarchicalTopic<VEC_CAP, STR_CAP>) -> Vec<T> {
        let parts = topic.parts_as_strings();
        let parts_deque: VecDeque<String> = VecDeque::from(parts);

        self.get_next(parts_deque)
    }

    pub fn clear(&mut self) {
        self.next.clear();
        self.value = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trie_index() {
        let mut trie_index = TrieIndexInternal::<String, 3, 3>::new();

        trie_index.insert_and_set_at_index(
            &HierarchicalIndex::<3, 3>::from_str("a.b.c").unwrap(),
            "value1".to_string(),
        );

        let result =
            trie_index.get_at_index(&HierarchicalTopic::<3, 3>::from_str("a.b.c").unwrap());
        assert_eq!(result, vec!["value1".to_string()]);
    }

    #[test]
    fn test_trie_index_with_wildcard() {
        let mut trie_index = TrieIndexInternal::<String, 3, 3>::new();

        trie_index.insert_and_set_at_index(
            &HierarchicalIndex::<3, 3>::from_str("a.*.c").unwrap(),
            "value1".to_string(),
        );

        let result =
            trie_index.get_at_index(&HierarchicalTopic::<3, 3>::from_str("a.b.c").unwrap());
        assert_eq!(result, vec!["value1".to_string()]);

        let result_nomatch =
            trie_index.get_at_index(&HierarchicalTopic::<3, 3>::from_str("a.b.d").unwrap());
        assert_eq!(result_nomatch.len(), 0);

        let result_nomatch_too_short =
            trie_index.get_at_index(&HierarchicalTopic::<3, 3>::from_str("a.b").unwrap());
        assert_eq!(result_nomatch_too_short.len(), 0);

        let result_nomatch_too_short2 =
            trie_index.get_at_index(&HierarchicalTopic::<3, 3>::from_str("a").unwrap());
        assert_eq!(result_nomatch_too_short2.len(), 0);
    }

    #[test]
    fn test_trie_index_clear() {
        let mut trie_index = TrieIndexInternal::<String, 3, 3>::new();

        trie_index.insert_and_set_at_index(
            &HierarchicalIndex::<3, 3>::from_str("a.b.c").unwrap(),
            "value1".to_string(),
        );
        trie_index.insert_and_set_at_index(
            &HierarchicalIndex::<3, 3>::from_str("a.b.*").unwrap(),
            "value1".to_string(),
        );
        trie_index.insert_and_set_at_index(
            &HierarchicalIndex::<3, 3>::from_str("a.*.*").unwrap(),
            "value1".to_string(),
        );

        trie_index.clear();

        let result =
            trie_index.get_at_index(&HierarchicalTopic::<3, 3>::from_str("a.b.c").unwrap());
        assert_eq!(result.len(), 0);
    }
}
