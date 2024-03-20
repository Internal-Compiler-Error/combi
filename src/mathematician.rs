#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Mathematician {
    pub name: String,
    pub dissertation: Option<String>,
    pub year: Option<u16>,
    pub school: Option<School>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct School {
    pub name: String,
    pub country: Option<String>,
}

pub struct MathematicianBuilder {
    name: String,
    dissertation: Option<String>,
    year: Option<u16>,
    school: Option<School>,
}

impl MathematicianBuilder {
    pub fn new() -> Self {
        Self {
            name: String::new(),
            dissertation: None,
            year: None,
            school: None,
        }
    }

    pub fn full_name(&mut self, name: String) -> &mut Self {
        self.name = name;
        self
    }

    pub fn first_name(&mut self, first_name: &str) -> &mut Self {
        self.name.push_str(&first_name);
        self
    }

    pub fn last_name(&mut self, last_name: &str) -> &mut Self {
        self.name.push_str(last_name);
        self
    }

    pub fn school(&mut self, school: School) -> &mut Self {
        self.school = Some(school);
        self
    }

    pub fn dissertation(&mut self, dissertation: String) -> &mut Self {
        self.dissertation = Some(dissertation);
        self
    }

    pub fn year(&mut self, year: u16) -> &mut Self {
        self.year = Some(year);
        self
    }

    pub fn build(self) -> Mathematician {
        Mathematician {
            name: self.name,
            school: self.school,
            dissertation: self.dissertation,
            year: self.year,
        }
    }
}
