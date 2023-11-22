use std::sync::Arc;

use bzip2::read::MultiBzDecoder;
use deadpool_postgres::{Client, Pool, Runtime};
use postgres::{error::SqlState, NoTls};
use tokio::sync::Mutex;
use xml::reader::{EventReader, XmlEvent};

use super::{
    index_builder::{self, IndexBuilder},
    snippet_engine::{self, SnippetEngine},
};

pub const QUERY_RETRY_LIMIT: u32 = 10;

enum Tag {
    Title,
    Id,
    Text,
    Other,
}

#[derive(Debug, Clone)]
pub struct Article {
    pub id: u32,
    pub title: String,
    pub text: String,
}

impl Article {
    fn new() -> Self {
        Article {
            title: String::new(),
            id: u32::MAX,
            text: String::new(),
        }
    }

    fn parsed_id(&self) -> bool {
        self.id != u32::MAX
    }
}

pub async fn build_index(
    wiki_dump_path: &String,
    db_config: &deadpool_postgres::Config,
) -> Result<usize, String> {
    let mut wiki_dump_file = std::fs::File::open(wiki_dump_path).unwrap();
    let mut reader = MultiBzDecoder::new(&mut wiki_dump_file);
    let parser = EventReader::new(&mut reader);

    match parse_dump(parser, db_config).await {
        Err(e) => Err(format!("Error parsing dump: {}", e)),
        Ok(article_count) => Ok(article_count),
    }
}

async fn parse_dump(
    parser: EventReader<&mut MultiBzDecoder<&mut std::fs::File>>,
    db_config: &deadpool_postgres::Config,
) -> Result<usize, String> {
    let db_connection_pool = match create_pool(db_config).map_err(|err| err.to_string()) {
        Ok(pool) => pool,
        Err(e) => return Err(format!("Error creating DB connection pool: {}", e)),
    };

    prepare_db(db_connection_pool.clone())
        .await
        .map_err(|e| format! {"Error preparing DB: {e}"})?;

    let mut cur_tag = Tag::Other;
    let mut cur_article = Article::new();
    let index_builder = Arc::new(Mutex::new(index_builder::IndexBuilder::new()));
    let snippet_engine = snippet_engine::SnippetEngine::new(db_connection_pool.clone())
        .await
        .map_err(|e| format! {"Error creating snippet engine: {e}"})?;

    let mut article_count = 0;

    // Let's parse the dump by streaming it (StAX) instead of loading it all into memory (DOM)
    // xml-rs does StAX out of the box so we're chilling
    for event in parser {
        match event {
            Ok(XmlEvent::StartElement {
                name,
                attributes: _,
                ..
            }) => {
                cur_tag = match name.local_name.as_str() {
                    "title" => Tag::Title,
                    "id" => Tag::Id,
                    "text" => Tag::Text,
                    _ => Tag::Other,
                };
            }
            Ok(XmlEvent::EndElement { name }) => {
                cur_tag = Tag::Other;
                if name.local_name.as_str() == "page" {
                    article_count += 1;
                    if let Err(e) = index_article(
                        // All these clones are fairly cheap
                        cur_article.clone(),
                        index_builder.clone(),
                        snippet_engine.clone(),
                    )
                    .await
                    {
                        eprintln!("Error indexing article {}: {}", cur_article.id, e);
                    }
                    if cur_article.id == 10000 {
                        // Limit to first 10k articles for now... :/
                        break;
                    }
                    cur_article = Article::new();
                }
            }
            Ok(XmlEvent::Characters(chars)) => match cur_tag {
                Tag::Title => {
                    cur_article.title = chars;
                }
                Tag::Id => {
                    if !cur_article.parsed_id() {
                        cur_article.id = chars.parse::<u32>().unwrap();
                    }
                }
                Tag::Text => {
                    cur_article.text = chars;
                }
                _ => {}
            },
            Err(e) => {
                return Err(format!(
                    "Failed to parse XML at index {}: {}",
                    cur_article.id, e
                ));
            }
            _ => {}
        }
    }

    index_builder
        .lock()
        .await
        .write_lexicon(db_connection_pool.clone())
        .await
        .map_err(|e| format!("Error writing lexicon: {}", e))?;

    index_builder
        .lock()
        .await
        .write_inverted_index(db_connection_pool)
        .await
        .map_err(|e| format!("Error writing inverted index: {e}"))?;

    Ok(article_count)
}

pub fn create_pool(db_config: &deadpool_postgres::Config) -> Result<Pool, String> {
    Ok(db_config
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .map_err(|err| err.to_string())?)
}

pub async fn get_connection(pool: &Pool) -> Result<Client, String> {
    pool.get()
        .await
        .map_err(|e| format!("Error getting connection: {}", e))
}

pub fn should_retry(sql_error: &postgres::Error) -> bool {
    sql_error
        .code()
        .map(|e| *e == SqlState::T_R_SERIALIZATION_FAILURE || *e == SqlState::T_R_DEADLOCK_DETECTED)
        .unwrap_or(false)
}

pub async fn execute_with_retry<T: ?Sized + postgres::ToStatement>(
    connection: &mut Client,
    query: &T,
    params: &[&(dyn postgres::types::ToSql + Sync)],
) -> Result<u64, String> {
    for _ in 0..QUERY_RETRY_LIMIT {
        let transaction = connection.transaction().await.map_err(|e| e.to_string())?;
        let result = transaction.execute(query, params).await;
        let result_commit = transaction.commit().await;

        if result_commit.is_err() && result_commit.map_err(|e| should_retry(&e)).unwrap_err() {
            continue;
        }

        match result {
            Ok(num_rows) => return Ok(num_rows),
            Err(e) => {
                if should_retry(&e) {
                    continue;
                } else {
                    return Err(format!("Error executing query: {}", e));
                }
            }
        }
    }

    return Err(format!("Error executing query: Reached retry limit"));
}

async fn index_article(
    article: Article,
    index_builder: Arc<Mutex<IndexBuilder>>,
    snippet_engine: SnippetEngine,
) -> Result<(), String> {
    if let Err(e) = snippet_engine.insert_article(&article).await {
        return Err(format!("Error inserting article: {}", e));
    }

    if let Err(e) = index_builder.lock().await.build_index(&article) {
        return Err(format!("Error building index: {}", e));
    }

    Ok(())
}

async fn prepare_db(db_connection_pool: Pool) -> Result<(), String> {
    let connection = get_connection(&db_connection_pool).await?;

    // create articles table
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS articles (
                article_id INT PRIMARY KEY,
                title TEXT NOT NULL,
                text MEDIUMTEXT NOT NULL
                length INT NOT NULL,
            )",
            &[],
        )
        .await
        .map_err(|err| err.to_string())?;

    // create lexicon table
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS lexicon (
                token_id INT PRIMARY KEY,
                token VARCHAR(255) NOT NULL,
            )",
            &[],
        )
        .await
        .map_err(|err| err.to_string())?;

    // create inverted index table
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS inverted_index (
                token_id INT NOT NULL,
                article_id INT NOT NULL,
                count INT NOT NULL,
                PRIMARY KEY (token_id, article_id),
                FOREIGN KEY (token_id) REFERENCES lexicon(token_id),
                FOREIGN KEY (article_id) REFERENCES articles(article_id),
            )",
            &[],
        )
        .await
        .map_err(|err| err.to_string())?;

    Ok(())
}
