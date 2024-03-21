use sqlx::FromRow;

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Dissertation {
    pub title: String,
    pub author: Mathematician,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Mathematician {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct School {
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Country {
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct GraduationRecord {
    pub mathematician: Mathematician,
    pub school: School,
    pub country: Option<Country>,
    pub year: u16,
}
