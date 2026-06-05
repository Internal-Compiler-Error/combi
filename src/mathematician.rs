use sqlx::FromRow;

use crate::parser::Id;

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Dissertation {
    pub title: String,
    pub author: Id,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Mathematician {
    pub id: Id,
    pub name: Option<String>,
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

// This type has issues, see the init sql file for more
#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct GraduationRecord {
    pub mathematician: Id,
    pub school: String,
    pub year: i32,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct AdvisorRelation {
    pub advisor: Id,
    pub advisee: Id,
}

pub trait MathematicianRepo {
    async fn mathematician_by_id(&self, id: Id) -> color_eyre::Result<Option<Mathematician>>;

    async fn upsert_mathematician(&self, mathematician: &Mathematician) -> color_eyre::Result<()>;
}

impl MathematicianRepo for sqlx::PgPool {
    async fn mathematician_by_id(&self, id: Id) -> color_eyre::Result<Option<Mathematician>> {
        sqlx::query_as!(
            Mathematician,
            "SELECT id, name FROM mathematicians WHERE id = $1",
            id.0
        )
        .fetch_optional(self)
        .await
        .map_err(Into::into)
    }

    async fn upsert_mathematician(&self, mathematician: &Mathematician) -> color_eyre::Result<()> {
        sqlx::query!(
            "INSERT INTO mathematicians (id, name) VALUES ($1, $2) ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name",
            &mathematician.id.0,
            mathematician.name,
        )
        .execute(self)
        .await?;

        Ok(())
    }
}

pub trait SchoolRepo {
    async fn school_by_name(&self, name: &str) -> color_eyre::Result<Option<School>>;

    async fn update_school(&self, school: &School) -> color_eyre::Result<()>;
}

impl SchoolRepo for sqlx::PgPool {
    async fn school_by_name(&self, name: &str) -> color_eyre::Result<Option<School>> {
        sqlx::query_as!(School, "SELECT name FROM schools WHERE name = $1", name)
            .fetch_optional(self)
            .await
            .map_err(Into::into)
    }

    async fn update_school(&self, school: &School) -> color_eyre::Result<()> {
        sqlx::query!(
            "INSERT INTO schools (name) VALUES ($1) ON CONFLICT DO NOTHING",
            &school.name
        )
        .execute(self)
        .await?;

        Ok(())
    }
}

pub trait CountryRepo {
    async fn country_by_name(&self, name: &str) -> color_eyre::Result<Option<Country>>;

    async fn update_country(&self, country: &Country) -> color_eyre::Result<()>;
}

impl CountryRepo for sqlx::PgPool {
    async fn country_by_name(&self, name: &str) -> color_eyre::Result<Option<Country>> {
        sqlx::query_as!(Country, "SELECT name FROM countries WHERE name = $1", name)
            .fetch_optional(self)
            .await
            .map_err(Into::into)
    }

    async fn update_country(&self, country: &Country) -> color_eyre::Result<()> {
        sqlx::query!(
            "INSERT INTO countries (name) VALUES ($1) ON CONFLICT DO NOTHING",
            &country.name
        )
        .execute(self)
        .await?;

        Ok(())
    }
}

pub trait AdvisorRelationRepo {
    async fn advisor_by_id(&self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>;

    async fn advisee_by_id(&self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>>;

    async fn update_advisor_relation(&self, relation: &AdvisorRelation) -> color_eyre::Result<()>;
}

impl AdvisorRelationRepo for sqlx::PgPool {
    async fn advisor_by_id(&self, advisor: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        sqlx::query_as!(
            AdvisorRelation,
            "SELECT advisor, advisee FROM advisor_relations WHERE advisor = $1",
            advisor.0
        )
        .fetch_all(self)
        .await
        .map(|relations: Vec<AdvisorRelation>| relations.into_boxed_slice())
        .map_err(Into::into)
    }

    async fn advisee_by_id(&self, advisee: Id) -> color_eyre::Result<Box<[AdvisorRelation]>> {
        sqlx::query_as!(
            AdvisorRelation,
            "SELECT advisor, advisee FROM advisor_relations WHERE advisee = $1",
            advisee.0
        )
        .fetch_all(self)
        .await
        .map(|relations: Vec<AdvisorRelation>| relations.into_boxed_slice())
        .map_err(Into::into)
    }

    async fn update_advisor_relation(&self, relation: &AdvisorRelation) -> color_eyre::Result<()> {
        sqlx::query!("INSERT INTO advisor_relations (advisor, advisee) VALUES ($1, $2) ON CONFLICT DO NOTHING", &relation.advisor.0, &relation.advisee.0)
            .execute(self)
            .await?;
        Ok(())
    }
}

pub trait SchoolLocationRepo {
    async fn schools_with_location(
        &self,
        school: &School,
    ) -> color_eyre::Result<Option<SchoolLocation>>;

    async fn update_location(&self, location: &SchoolLocation) -> color_eyre::Result<()>;
}

impl SchoolLocationRepo for sqlx::PgPool {
    async fn schools_with_location(
        &self,
        school: &School,
    ) -> color_eyre::Result<Option<SchoolLocation>> {
        sqlx::query_as!(
            SchoolLocation,
            "SELECT school, country FROM school_locations WHERE school = $1",
            school.name
        )
        .fetch_optional(self)
        .await
        .map_err(Into::into)
    }

    async fn update_location(&self, location: &SchoolLocation) -> color_eyre::Result<()> {
        sqlx::query!(
            "INSERT INTO school_locations (school, country) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &location.school,
            &location.country
        )
        .execute(self)
        .await?;
        Ok(())
    }
}

pub trait DissertationRepo {
    async fn dissertation_by_author(&self, author: Id) -> color_eyre::Result<Option<Dissertation>>;

    async fn upsert_dissertation(&self, dissertation: &Dissertation) -> color_eyre::Result<()>;
}

impl DissertationRepo for sqlx::PgPool {
    async fn dissertation_by_author(&self, author: Id) -> color_eyre::Result<Option<Dissertation>> {
        sqlx::query_as!(
            Dissertation,
            "SELECT author, title FROM dissertations WHERE author = $1 AND title IS NOT NULL",
            author.0
        )
        .fetch_optional(self)
        .await
        .map_err(Into::into)
    }

    async fn upsert_dissertation(&self, dissertation: &Dissertation) -> color_eyre::Result<()> {
        sqlx::query!(
            // "INSERT INTO dissertations (author, title) VALUES ($1, $2) ON CONFLICT(author) DO UPDATE SET title = EXCLUDED.title",
            "INSERT INTO dissertations (author, title) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &dissertation.author.0,
            &dissertation.title
        )
        .execute(self)
        .await?;

        Ok(())
    }
}

pub trait GraduationRecordRepo {
    async fn graduation_record_by_mathematician(
        &self,
        mathematician: Id,
    ) -> color_eyre::Result<Option<GraduationRecord>>;
    async fn upsert_graduation_record(&self, record: &GraduationRecord) -> color_eyre::Result<()>;
}

impl GraduationRecordRepo for sqlx::PgPool {
    async fn graduation_record_by_mathematician(
        &self,
        mathematician: Id,
    ) -> color_eyre::Result<Option<GraduationRecord>> {
        let map = sqlx::query_as!(
            GraduationRecord,
            "SELECT mathematician, school, year FROM graduation_records WHERE mathematician = $1",
            mathematician.0
        );
        map.fetch_optional(self).await.map_err(Into::into)
    }
    async fn upsert_graduation_record(&self, record: &GraduationRecord) -> color_eyre::Result<()> {
        // TODO: actually upsert instead of 'do nothing'
        sqlx::query!("INSERT INTO graduation_records (mathematician, school, year) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", &record.mathematician.0, &record.school, &record.year)
            .execute(self)
            .await?;
        Ok(())
    }
}
