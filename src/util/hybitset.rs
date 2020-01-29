use std::collections::HashMap;

const SEGMENT_USIZE_LEN: usize = 128;
const BITS_PER_USIZE:    usize = 8 * std::mem::size_of::<usize>();
const SEGMENT_LEN:       usize = SEGMENT_USIZE_LEN * BITS_PER_USIZE;

struct Segment([usize; SEGMENT_USIZE_LEN]);

impl Segment {
    pub fn new() -> Segment {
        Segment([0; SEGMENT_USIZE_LEN])
    }

    #[inline]
    fn get_usize(&mut self, segment_index: usize) -> &mut usize {
        let usize_index = segment_index / BITS_PER_USIZE;
        let usize_val = &mut self.0[usize_index];
        usize_val
    }

    #[inline]
    fn get_mask(segment_index: usize) -> usize {
        let mask = 1_usize << (segment_index % BITS_PER_USIZE);
        mask
    }

    pub fn insert(&mut self, segment_index: usize) -> bool {
        let mask = Self::get_mask(segment_index);
        let usize_val = self.get_usize(segment_index);

        let out = 0 != (mask & *usize_val);
        *usize_val |= mask;
        out
    }

    pub fn remove(&mut self, segment_index: usize) -> bool {
        let mask = Self::get_mask(segment_index);
        let usize_val = self.get_usize(segment_index);

        let out = 0 != (mask & *usize_val);
        *usize_val &= !mask;
        out
    }

    pub fn contains(&mut self, segment_index: usize) -> bool {
        let mask = Self::get_mask(segment_index);
        let usize_val = self.get_usize(segment_index);

        let out = 0 != (mask & *usize_val);
        out
    }
}

pub struct HyBitSet {
    segment_map: HashMap<usize, Segment>,
    len: usize,
}

impl HyBitSet {
    #[allow(dead_code)]
    pub fn new() -> HyBitSet {
        HyBitSet {
            segment_map: HashMap::new(),
            len: 0,
        }
    }

    #[inline]
    fn get_seg(&mut self, val: usize) -> &mut Segment {
        let seg_id  = val / SEGMENT_LEN;
        let segment = self.segment_map.entry(seg_id).or_insert(Segment::new());
        segment
    }

    #[inline]
    fn get_off(val: usize) -> usize {
        let seg_off = val % SEGMENT_LEN;
        seg_off
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    pub fn insert(&mut self, val: usize) -> bool {
        let off = Self::get_off(val);
        let seg = self.get_seg(val);
        if seg.insert(off) {
            true
        }
        else {
            self.len += 1;
            false
        }
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, val: usize) -> bool {
        let off = Self::get_off(val);
        let seg = self.get_seg(val);
        if seg.remove(off) {
            self.len -= 1;
            true
        }
        else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn contains(&mut self, val: usize) -> bool {
        let off = Self::get_off(val);

        let seg_id = val / SEGMENT_LEN;

        match self.segment_map.get_mut(&seg_id) {
            Some(seg) => seg.contains(off),
            None => false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic() {
        let mut bs = HyBitSet::new();
        assert_eq!(0, bs.len());

        assert!(false == bs.insert(126_usize));
        assert!(false == bs.insert(127_usize));
        assert_eq!(2, bs.len());

        assert!(true  == bs.contains(127_usize));
        assert!(false == bs.contains(0_usize));

        assert!(true  == bs.remove(127_usize));
        assert!(false == bs.contains(127_usize));
        assert_eq!(1, bs.len());

        assert!(true  == bs.contains(126_usize));
    }

    #[test]
    fn test_large() {
        let mut bs = HyBitSet::new();
        assert_eq!(0, bs.len());

        assert!(false == bs.insert(3_617_178_774_usize));
        assert!(false == bs.insert(3_651_972_316_usize));
        assert_eq!(2, bs.len());

        assert!(true  == bs.contains(3_651_972_316_usize));
        assert!(false == bs.contains(0_usize));

        assert!(true  == bs.remove(3_651_972_316_usize));
        assert!(false == bs.contains(3_651_972_316_usize));
        assert_eq!(1, bs.len());

        assert!(true  == bs.contains(3_617_178_774_usize));
    }

    #[test]
    fn test_wide() {
        let mut bs = HyBitSet::new();
        assert_eq!(0, bs.len());

        assert!(false == bs.insert(5_usize));
        assert!(false == bs.insert(3_651_972_316_usize));
        assert_eq!(2, bs.len());

        assert!(true  == bs.contains(3_651_972_316_usize));
        assert!(false == bs.contains(0_usize));

        assert!(true  == bs.remove(3_651_972_316_usize));
        assert!(false == bs.contains(3_651_972_316_usize));
        assert_eq!(1, bs.len());

        assert!(true  == bs.contains(5_usize));
    }
}
