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
    pub year: Option<u16>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct School {
    pub name: String,
}
