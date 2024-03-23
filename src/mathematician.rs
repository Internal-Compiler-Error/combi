use sqlx::FromRow;

use crate::parser::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Dissertation {
    pub title: String,
    pub author: Mathematician,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Mathematician {
    pub id: Id,
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct School {
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct SchoolLocation {
    pub school: School,
    pub country: Country,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Country {
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct GraduationRecord {
    pub mathematician: Mathematician,
    pub school: School,
    pub year: i32,
}
