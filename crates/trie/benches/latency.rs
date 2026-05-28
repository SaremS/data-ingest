use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use trie::{
    hierarchical_index::{HierarchicalIndex, HierarchicalTopic},
    trie_index::TrieIndex,
};

const VEC_CAP: usize = 8;
const STR_CAP: usize = 32;
const TOPIC_PARTS: [&str; 5] = ["feed", "nasdaq", "equities", "aapl", "quote"];

fn topic(parts: &[&str]) -> HierarchicalTopic<VEC_CAP, STR_CAP> {
    HierarchicalTopic::from_str(&parts.join(".")).expect("valid topic")
}

fn index(parts: &[&str]) -> HierarchicalIndex<VEC_CAP, STR_CAP> {
    HierarchicalIndex::from_str(&parts.join(".")).expect("valid index")
}

fn wildcard_index(mask: usize) -> HierarchicalIndex<VEC_CAP, STR_CAP> {
    let pattern = TOPIC_PARTS
        .iter()
        .enumerate()
        .map(|(position, part)| {
            if (mask & (1 << position)) == 0 {
                "*"
            } else {
                part
            }
        })
        .collect::<Vec<_>>()
        .join(".");

    HierarchicalIndex::from_str(&pattern).expect("valid wildcard index")
}

fn seeded_trie(match_count: usize) -> TrieIndex<u64, VEC_CAP, STR_CAP> {
    let trie = TrieIndex::new();

    for value in 0..match_count {
        trie.insert_and_set_at_index(&wildcard_index(value + 1), value as u64);
    }

    trie
}

fn lookup_benches(c: &mut Criterion) {
    let exact_topic = topic(&TOPIC_PARTS);
    let exact_index = index(&TOPIC_PARTS);
    let mut group = c.benchmark_group("trie_lookup");

    group.bench_function("exact_single_match", |b| {
        let trie = TrieIndex::new();
        trie.insert_and_set_at_index(&exact_index, 1_u64);

        b.iter(|| {
            black_box(trie.get_at_index(black_box(&exact_topic)));
        });
    });

    for match_count in [4_usize, 8, 16, 32] {
        group.throughput(Throughput::Elements(match_count as u64));
        group.bench_with_input(
            BenchmarkId::new("wildcard_fanout", match_count),
            &match_count,
            |b, &count| {
                let trie = seeded_trie(count);

                b.iter(|| {
                    black_box(trie.get_at_index(black_box(&exact_topic)));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(latency, lookup_benches);
criterion_main!(latency);
