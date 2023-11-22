use deadpool_postgres::Pool;
use postgres::Statement;

use super::index_engine::{execute_with_retry, get_connection, Article};

#[derive(Clone)]
pub struct SnippetEngine {
    pub db_connection_pool: Pool,
    pub insert_article_statement: Statement,
}

impl SnippetEngine {
    pub async fn new(db_connection_pool: Pool) -> Result<Self, String> {
        let connection = get_connection(&db_connection_pool).await?;

        // not sure if preparing the statment beforehand improved performance...
        let insert_article_statement = match connection
            .prepare("INSERT INTO articles (id, title, text) VALUES ($1, $2, $3)")
            .await
        {
            Ok(query) => query,
            Err(e) => return Err(format!("Error preparing insert statement: {}", e)),
        };

        Ok(SnippetEngine {
            db_connection_pool,
            insert_article_statement,
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
}
