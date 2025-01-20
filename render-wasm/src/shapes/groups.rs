#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Group {
    masked: bool
}

impl Group {
    pub fn new(masked: bool) -> Self {
        Group {
          masked
        }
    }
}
