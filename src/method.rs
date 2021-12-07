#[derive(Debug, Copy, Clone)]
pub(crate) enum Method {
    Put,
    Del,
}

impl Default for Method {
    fn default() -> Self {
        Self::Put
    }
}

impl From<u16> for Method {
    fn from(i: u16) -> Method {
        match i {
            0 => Self::Put,
            1 => Self::Del,
            _ => panic!("unknown num: {}", i),
        }
    }
}

impl From<Method> for u16 {
    fn from(i: Method) -> u16 {
        match i {
            Method::Put => 0,
            Method::Del => 1,
        }
    }
}
