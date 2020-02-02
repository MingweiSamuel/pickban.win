#[derive(Debug)]
pub struct PbwError {
    msg: String
}

impl PbwError {
    pub fn new(msg: String) -> Self {
        Self {
            msg: msg,
        }
    }
}

impl std::fmt::Display for PbwError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for PbwError {}


