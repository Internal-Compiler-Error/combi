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
    pub mathematician: Id,
    pub school: School,
    pub year: i32,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct AdvisorRelation {
    pub advisor: Id,
    pub advisee: Id,
}

pub trait MathematicainRepo {
    async fn find_by_id(&self, id: Id) -> color_eyre::Result<Option<Mathematician>>;

    async fn update(&self, mathematician: &Mathematician) -> color_eyre::Result<()>;
}

impl MathematicainRepo for sqlx::PgPool {
    async fn find_by_id(&self, id: Id) -> color_eyre::Result<Option<Mathematician>> {
        sqlx::query_as("SELECT id, name FROM mathematicians WHERE id = $1")
            .bind(id)
            .fetch_optional(self)
            .await
            .map_err(Into::into)
    }

    async fn update(&self, mathematician: &Mathematician) -> color_eyre::Result<()> {
        sqlx::query("INSERT INTO mathematicians (id, name) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET name = $1")
            .bind(&mathematician.id)
            .bind(&mathematician.name)
            .execute(self)
            .await?;

        Ok(())
    }
}

pub trait SchoolRepo {
    async fn find_by_name(&self, name: &str) -> color_eyre::Result<Option<School>>;

    async fn update(&self, school: &School) -> color_eyre::Result<()>;
}

impl SchoolRepo for sqlx::PgPool {
    async fn find_by_name(&self, name: &str) -> color_eyre::Result<Option<School>> {
        sqlx::query_as("SELECT name FROM schools WHERE name = $1")
            .bind(name)
            .fetch_optional(self)
            .await
            .map_err(Into::into)
    }

    async fn update(&self, school: &School) -> color_eyre::Result<()> {
        sqlx::query("INSERT INTO school (name) VALUES ($1) ON CONFLICT (id) DO NOTHING")
            .bind(&school.name)
            .execute(self)
            .await?;

        Ok(())
    }
}

pub trait CountryRepo {
    async fn find_by_name(&self, name: &str) -> color_eyre::Result<Option<Country>>;

    async fn update(&self, country: &Country) -> color_eyre::Result<()>;
}

impl CountryRepo for sqlx::PgPool {
    async fn find_by_name(&self, name: &str) -> color_eyre::Result<Option<Country>> {
        sqlx::query_as("SELECT name FROM countries WHERE name = $1")
            .bind(name)
            .fetch_optional(self)
            .await
            .map_err(Into::into)
    }

    async fn update(&self, country: &Country) -> color_eyre::Result<()> {
        sqlx::query("INSERT INTO countries (name) VALUES ($1) ON CONFLICT (id) DO NOTHING")
            .bind(&country.name)
            .execute(self)
            .await?;

        Ok(())
    }
}

pub trait AdvisorRelationRepo {
    async fn find_by_advisor(&self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>;

    async fn find_by_advisee(&self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>;

    async fn update(&self, relation: &AdvisorRelation) -> color_eyre::Result<()>;
}

impl AdvisorRelationRepo for sqlx::PgPool {
    async fn find_by_advisor(&self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        sqlx::query_as("SELECT advisor, advisee FROM advisor_relations WHERE advisor = $1")
            .bind(advisor)
            .fetch_all(self)
            .await
            .map(|relations: Vec<AdvisorRelation>| relations.into_boxed_slice())
            .map_err(Into::into)
    }

    async fn find_by_advisee(&self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        sqlx::query_as("SELECT advisor, advisee FROM advisor_relations WHERE advisee = $1")
            .bind(advisee)
            .fetch_all(self)
            .await
            .map(|relations: Vec<AdvisorRelation>| relations.into_boxed_slice())
            .map_err(Into::into)
    }

    async fn update(&self, relation: &AdvisorRelation) -> color_eyre::Result<()> {
        sqlx::query("INSERT INTO advisor_relations (advisor, advisee) VALUES ($1, $2) ON CONFLICT (advisor, advisee) DO NOTHING")
            .bind(&relation.advisor)
            .bind(&relation.advisee)
            .execute(self)
            .await?;
        Ok(())
    }
}
