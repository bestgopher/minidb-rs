use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::method::Method;
use crate::{Error, Result};

pub(crate) const HEADER_SIZE: u64 = 18u64;

#[derive(Default, Debug)]
pub(crate) struct Entry {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) key_size: u64,
    pub(crate) value_size: u64,
    pub(crate) mark: Method,
}

impl Entry {
    pub(crate) fn new(key: Bytes, value: Bytes, mark: Method) -> Entry {
        let (key_size, value_size) = (key.len() as u64, value.len() as u64);
        Entry {
            key,
            value,
            mark,
            key_size,
            value_size,
        }
    }

    #[inline]
    pub(crate) const fn get_size(&self) -> u64 {
        HEADER_SIZE + self.key_size + self.value_size
    }

    pub(crate) fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(self.get_size() as usize);
        buf.put_u64(self.key_size);
        buf.put_u64(self.value_size);
        buf.put_u16(self.mark.into());
        buf.put_slice(self.key.as_ref());
        buf.put_slice(self.value.as_ref());
        Ok(Bytes::from(buf))
    }

    pub(crate) fn decode(other: &[u8]) -> Result<Entry> {
        if other.is_empty() {
            return Err(Error::EmptyContent);
        }

        let key_size = (&other[..8]).get_u64();
        let value_size = (&other[8..16]).get_u64();
        let mark = (&other[16..18]).get_u16();

        Ok(Entry {
            key_size,
            value_size,
            mark: mark.into(),
            ..Default::default()
        })
    }
}
