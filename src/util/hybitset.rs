use std::collections::HashMap;

use serde::{Deserialize, Serialize, Serializer };

const SEGMENT_BYTE_LEN: usize = 1024;
const BITS_PER_BYTE:    usize = 8;
const SEGMENT_LEN:      usize = SEGMENT_BYTE_LEN * BITS_PER_BYTE;

struct Segment([u8; SEGMENT_BYTE_LEN]);

impl Segment {
    pub fn new() -> Segment {
        Segment([0; SEGMENT_BYTE_LEN])
    }

    #[inline]
    fn get_byte(&mut self, segment_index: usize) -> &mut u8 {
        let byte_index = segment_index / BITS_PER_BYTE;
        let byte_val = &mut self.0[byte_index];
        byte_val
    }

    #[inline]
    fn get_mask(segment_index: usize) -> u8 {
        let mask = 1_u8 << (segment_index % BITS_PER_BYTE);
        mask
    }

    pub fn insert(&mut self, segment_index: usize) -> bool {
        let mask = Self::get_mask(segment_index);
        let byte_val = self.get_byte(segment_index);

        let out = 0 != (mask & *byte_val);
        *byte_val |= mask;
        out
    }

    pub fn remove(&mut self, segment_index: usize) -> bool {
        let mask = Self::get_mask(segment_index);
        let byte_val = self.get_byte(segment_index);

        let out = 0 != (mask & *byte_val);
        *byte_val &= !mask;
        out
    }

    pub fn contains(&mut self, segment_index: usize) -> bool {
        let mask = Self::get_mask(segment_index);
        let byte_val = self.get_byte(segment_index);

        let out = 0 != (mask & *byte_val);
        out
    }
}

impl Serialize for Segment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        base64::encode(&self.0[..]).serialize(serializer)
    }
}

#[derive(Serialize)]//, Deserialize)]
pub struct HyBitSet {
    len: usize,
    segment_byte_len: usize,
    segment_map: HashMap<usize, Segment>,
}

impl HyBitSet {
    #[allow(dead_code)]
    pub fn new() -> HyBitSet {
        HyBitSet {
            len: 0,
            segment_byte_len: SEGMENT_BYTE_LEN,
            segment_map: HashMap::new(),
        }
    }

    #[inline]
    fn get_seg(&mut self, val: usize) -> &mut Segment {
        let seg_id  = val - (val % SEGMENT_LEN);
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

    pub fn density(&self) -> f32 {
        (self.len as f32) / (self.segment_map.len() as f32)
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

        let seg_id  = val - (val % SEGMENT_LEN);
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
