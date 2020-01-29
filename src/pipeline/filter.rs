use std::collections::BinaryHeap;
use std::iter::IntoIterator;

pub fn filter_min_n<I, T>(limit: usize, iter: I) -> BinaryHeap<T> where
    I: IntoIterator<Item = T>,
    T: Ord,
{
    let mut heap = BinaryHeap::with_capacity(limit);

    for item in iter {
        // If we're full we'll need to pop.
        if heap.len() >= limit {
            // But if the item is already larger than the largest
            // item in the heap, then we must ignore the item.
            if item >= *heap.peek().unwrap() {
                continue;
            }
            heap.pop();
        }
        heap.push(item);
    }

    heap
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_filter_min_n() {
        let values = vec![
            Some(5), Some(2), Some(-10), None, Some(12), Some(4),
            Some(15), Some(-15), Some(0), Some(-10), None, Some(-1) ];
        let min_values = filter_min_n(5, values);
        println!("{:?}", min_values.into_iter().collect::<Vec<_>>());
    }
}
