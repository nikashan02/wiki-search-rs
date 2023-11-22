use deadpool_postgres::Pool;
use postgres::Statement;

use super::index_engine::{execute_with_retry, get_connection, Article};

#[derive(Clone)]
pub struct SnippetEngine {
    pub db_connection_pool: Pool,
    pub insert_article_statement: Statement,
    pub update_length_statement: Statement,
}

impl SnippetEngine {
    pub async fn new(db_connection_pool: Pool) -> Result<Self, String> {
        let connection = get_connection(&db_connection_pool).await?;

        // not sure if preparing the statment beforehand improved performance...
        let insert_article_statement = connection
            .prepare(
                "INSERT INTO articles (article_id, title, text, length) VALUES ($1, $2, $3, 0)",
            )
            .await
            .map_err(|e| format!("Error preparing insert statement: {e}"))?;

        let update_length_statement = connection
            .prepare("UPDATE articles SET length = $1 WHERE article_id = $2")
            .await
            .map_err(|e| format!("Error preparing update length statement: {e}"))?;

        Ok(SnippetEngine {
            db_connection_pool,
            insert_article_statement,
            update_length_statement,
        })
    }

    pub async fn insert_article(&self, article: &Article) -> Result<(), String> {
        let mut connection = get_connection(&self.db_connection_pool)
            .await
            .map_err(|e: String| format!("Error inserting article: {e}"))?;

        execute_with_retry(
            &mut connection,
            &self.insert_article_statement,
            &[&article.id, &article.title, &article.text],
        )
        .await
        .map_err(|e| format!("Error inserting article: {e}"))?;

        Ok(())
    }

    pub async fn update_length(&self, article_id: i32, length: i32) -> Result<(), String> {
        let mut connection = get_connection(&self.db_connection_pool)
            .await
            .map_err(|e: String| format!("Error updating length: {e}"))?;

        execute_with_retry(
            &mut connection,
            &self.update_length_statement,
            &[&length, &article_id],
        )
        .await
        .map_err(|e| format!("Error updating length: {e}"))?;

        Ok(())
    }
}
