#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Mathematician {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub university: Option<String>,
    pub dissertation: Option<String>,
}

pub struct MathematicianBuilder {
    first_name: Option<String>,
    last_name: Option<String>,
    university: Option<String>,
    dissertation: Option<String>,
}

impl MathematicianBuilder {
    pub fn new() -> Self {
        Self {
            first_name: None,
            last_name: None,
            university: None,
            dissertation: None,
        }
    }

    pub fn first_name(mut self, first_name: String) -> Self {
        self.first_name = Some(first_name);
        self
    }

    pub fn last_name(mut self, last_name: String) -> Self {
        self.last_name = Some(last_name);
        self
    }

    pub fn university(mut self, university: String) -> Self {
        self.university = Some(university);
        self
    }

    pub fn dissertation(mut self, dissertation: String) -> Self {
        self.dissertation = Some(dissertation);
        self
    }

    pub fn build(self) -> Mathematician {
        Mathematician {
            first_name: self.first_name,
            last_name: self.last_name,
            university: self.university,
            dissertation: self.dissertation,
        }
    }
}

impl Mathematician {
    pub fn new(first_name: String, last_name: String, university: String) -> Self {
        Self {
            first_name: Some(first_name),
            last_name: Some(last_name),
            university: Some(university),
            dissertation: None,
        }
    }
}
