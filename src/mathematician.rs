use sqlx::{Executor, FromRow, Pool, Postgres};

use crate::parser::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Mathematician {
    pub id: Id,
    pub name: Option<String>,
    pub dissertation: Option<String>,
    pub graduating_year: Option<i32>,
    pub school: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct School {
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct SchoolLocation {
    pub school: String,
    pub country: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Country {
    pub name: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct AdvisorRelation {
    pub advisor: Id,
    pub advisee: Id,
}

// The repo traits below take `self` by value to match `sqlx::Executor`'s receiver. The
// query logic lives in these free functions, each generic over a single executor lifetime
// `'e` (a plain elided/named lifetime, NOT `for<'a>`). That bound is satisfiable because
// `E` is one concrete executor type used at one lifetime — exactly how sqlx implements
// `Executor`. The trait impls for `&mut PgConnection` and `&Pool<Postgres>` are then thin
// forwarders, so both receivers share one body without a `for<'a>` blanket impl (which is
// unsatisfiable: no single executor type is an `Executor` for every lifetime).

async fn mathematician_by_id<'e, E>(executor: E, id: Id) -> color_eyre::Result<Option<Mathematician>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(Mathematician, "SELECT * FROM mathematicians WHERE id = $1", id.0)
        .fetch_optional(executor)
        .await
        .map_err(Into::into)
}

async fn upsert_mathematician<'e, E>(executor: E, mathematician: &Mathematician) -> color_eyre::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        "INSERT INTO mathematicians (id, name, dissertation, graduating_year, school) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(id)
            DO UPDATE
                SET name = EXCLUDED.name,
                    dissertation = EXCLUDED.dissertation,
                    graduating_year = EXCLUDED.graduating_year,
                    school = EXCLUDED.school",
        &mathematician.id.0,
        mathematician.name,
        mathematician.dissertation,
        mathematician.graduating_year,
        mathematician.school
    )
    .execute(executor)
    .await?;

    Ok(())
}

async fn school_by_name<'e, E>(executor: E, name: &str) -> color_eyre::Result<Option<School>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(School, "SELECT name FROM schools WHERE name = $1", name)
        .fetch_optional(executor)
        .await
        .map_err(Into::into)
}

async fn update_school<'e, E>(executor: E, school: &School) -> color_eyre::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        "INSERT INTO schools (name) VALUES ($1) ON CONFLICT DO NOTHING",
        &school.name
    )
    .execute(executor)
    .await?;

    Ok(())
}

async fn country_by_name<'e, E>(executor: E, name: &str) -> color_eyre::Result<Option<Country>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(Country, "SELECT name FROM countries WHERE name = $1", name)
        .fetch_optional(executor)
        .await
        .map_err(Into::into)
}

async fn update_country<'e, E>(executor: E, country: &Country) -> color_eyre::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        "INSERT INTO countries (name) VALUES ($1) ON CONFLICT DO NOTHING",
        &country.name
    )
    .execute(executor)
    .await?;

    Ok(())
}

async fn advisor_by_id<'e, E>(executor: E, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        AdvisorRelation,
        "SELECT advisor, advisee FROM advisor_relations WHERE advisor = $1",
        advisor.0
    )
    .fetch_all(executor)
    .await
    .map(|relations: Vec<AdvisorRelation>| relations.into_boxed_slice())
    .map_err(Into::into)
}

async fn advisee_by_id<'e, E>(executor: E, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        AdvisorRelation,
        "SELECT advisor, advisee FROM advisor_relations WHERE advisee = $1",
        advisee.0
    )
    .fetch_all(executor)
    .await
    .map(|relations: Vec<AdvisorRelation>| relations.into_boxed_slice())
    .map_err(Into::into)
}

async fn update_advisor_relation<'e, E>(executor: E, relation: &AdvisorRelation) -> color_eyre::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        "INSERT INTO advisor_relations (advisor, advisee) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &relation.advisor.0,
        &relation.advisee.0
    )
    .execute(executor)
    .await?;

    Ok(())
}

async fn schools_with_location<'e, E>(executor: E, school: &School) -> color_eyre::Result<Option<SchoolLocation>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as!(
        SchoolLocation,
        "SELECT school, country FROM school_locations WHERE school = $1",
        school.name
    )
    .fetch_optional(executor)
    .await
    .map_err(Into::into)
}

async fn update_location<'e, E>(executor: E, location: &SchoolLocation) -> color_eyre::Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query!(
        "INSERT INTO school_locations (school, country) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &location.school,
        &location.country
    )
    .execute(executor)
    .await?;

    Ok(())
}

pub trait MathematicianRepo {
    async fn mathematician_by_id(self, id: Id) -> color_eyre::Result<Option<Mathematician>>;

    async fn upsert_mathematician(self, mathematician: &Mathematician) -> color_eyre::Result<()>;
}

impl MathematicianRepo for &mut sqlx::PgConnection {
    async fn mathematician_by_id(self, id: Id) -> color_eyre::Result<Option<Mathematician>> {
        mathematician_by_id(self, id).await
    }

    async fn upsert_mathematician(self, mathematician: &Mathematician) -> color_eyre::Result<()> {
        upsert_mathematician(self, mathematician).await
    }
}

impl MathematicianRepo for &Pool<Postgres> {
    async fn mathematician_by_id(self, id: Id) -> color_eyre::Result<Option<Mathematician>> {
        mathematician_by_id(self, id).await
    }

    async fn upsert_mathematician(self, mathematician: &Mathematician) -> color_eyre::Result<()> {
        upsert_mathematician(self, mathematician).await
    }
}

pub trait SchoolRepo {
    async fn school_by_name(self, name: &str) -> color_eyre::Result<Option<School>>;

    async fn update_school(self, school: &School) -> color_eyre::Result<()>;
}

impl SchoolRepo for &mut sqlx::PgConnection {
    async fn school_by_name(self, name: &str) -> color_eyre::Result<Option<School>> {
        school_by_name(self, name).await
    }

    async fn update_school(self, school: &School) -> color_eyre::Result<()> {
        update_school(self, school).await
    }
}

impl SchoolRepo for &Pool<Postgres> {
    async fn school_by_name(self, name: &str) -> color_eyre::Result<Option<School>> {
        school_by_name(self, name).await
    }

    async fn update_school(self, school: &School) -> color_eyre::Result<()> {
        update_school(self, school).await
    }
}

pub trait CountryRepo {
    async fn country_by_name(self, name: &str) -> color_eyre::Result<Option<Country>>;

    async fn update_country(self, country: &Country) -> color_eyre::Result<()>;
}

impl CountryRepo for &mut sqlx::PgConnection {
    async fn country_by_name(self, name: &str) -> color_eyre::Result<Option<Country>> {
        country_by_name(self, name).await
    }

    async fn update_country(self, country: &Country) -> color_eyre::Result<()> {
        update_country(self, country).await
    }
}

impl CountryRepo for &Pool<Postgres> {
    async fn country_by_name(self, name: &str) -> color_eyre::Result<Option<Country>> {
        country_by_name(self, name).await
    }

    async fn update_country(self, country: &Country) -> color_eyre::Result<()> {
        update_country(self, country).await
    }
}

pub trait AdvisorRelationRepo {
    async fn advisor_by_id(self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>;

    async fn advisee_by_id(self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>;

    async fn update_advisor_relation(self, relation: &AdvisorRelation) -> color_eyre::Result<()>;
}

impl AdvisorRelationRepo for &mut sqlx::PgConnection {
    async fn advisor_by_id(self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        advisor_by_id(self, advisor).await
    }

    async fn advisee_by_id(self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        advisee_by_id(self, advisee).await
    }

    async fn update_advisor_relation(self, relation: &AdvisorRelation) -> color_eyre::Result<()> {
        update_advisor_relation(self, relation).await
    }
}

impl AdvisorRelationRepo for &Pool<Postgres> {
    async fn advisor_by_id(self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        advisor_by_id(self, advisor).await
    }

    async fn advisee_by_id(self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        advisee_by_id(self, advisee).await
    }

    async fn update_advisor_relation(self, relation: &AdvisorRelation) -> color_eyre::Result<()> {
        update_advisor_relation(self, relation).await
    }
}

pub trait SchoolLocationRepo {
    async fn schools_with_location(
        self,
        school: &School,
    ) -> color_eyre::Result<Option<SchoolLocation>>;

    async fn update_location(self, location: &SchoolLocation) -> color_eyre::Result<()>;
}

impl SchoolLocationRepo for &mut sqlx::PgConnection {
    async fn schools_with_location(
        self,
        school: &School,
    ) -> color_eyre::Result<Option<SchoolLocation>> {
        schools_with_location(self, school).await
    }

    async fn update_location(self, location: &SchoolLocation) -> color_eyre::Result<()> {
        update_location(self, location).await
    }
}

impl SchoolLocationRepo for &Pool<Postgres> {
    async fn schools_with_location(
        self,
        school: &School,
    ) -> color_eyre::Result<Option<SchoolLocation>> {
        schools_with_location(self, school).await
    }

    async fn update_location(self, location: &SchoolLocation) -> color_eyre::Result<()> {
        update_location(self, location).await
    }
}
