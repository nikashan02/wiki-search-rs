use std::sync::Arc;

use bzip2::read::MultiBzDecoder;
use tokio::sync::Mutex;
use xml::reader::{EventReader, XmlEvent};

use crate::common::Article;

use super::{index_builder::IndexBuilder, snippet_engine};

const MAX_TASKS: usize = 50;
// Limit to the first 10000 articles for now... :/
const MAX_ARTICLES: usize = 10000;

enum Tag {
    Title,
    Id,
    Text,
    Other,
}

pub async fn build_index(wiki_dump_path: &String, index_path: &String) -> Result<usize, String> {
    let mut wiki_dump_file = std::fs::File::open(wiki_dump_path).unwrap();
    let mut reader = MultiBzDecoder::new(&mut wiki_dump_file);
    let parser = EventReader::new(&mut reader);

    match parse_dump(parser, index_path).await {
        Err(e) => Err(format!("Error parsing dump: {}", e)),
        Ok(article_count) => Ok(article_count),
    }
}

async fn parse_dump(
    parser: EventReader<&mut MultiBzDecoder<&mut std::fs::File>>,
    index_path: &String,
) -> Result<usize, String> {
    let mut cur_tag = Tag::Other;
    let mut cur_article = Article::new();

    let index_builder = Arc::new(Mutex::new(
        IndexBuilder::new(index_path).map_err(|e| format! {"Error creating index builder: {e}"})?,
    ));

    let mut article_count = 0;
    let mut tasks = Vec::new();

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

                    tasks.push(index_article(
                        // All these clones are fairly cheap
                        cur_article.clone(),
                        index_path.clone(),
                        index_builder.clone(),
                    ));

                    // Don't want to use up too much memory
                    if tasks.len() >= MAX_TASKS {
                        while let Some(task) = tasks.pop() {
                            if let Err(e) = task.await {
                                eprintln!("Error indexing article: {}", e);
                            }
                        }
                    }

                    if article_count >= MAX_ARTICLES {
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
                        cur_article.id = chars.parse::<usize>().unwrap();
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

    while let Some(task) = tasks.pop() {
        if let Err(e) = task.await {
            eprintln!("Error indexing article: {}", e);
        }
    }

    index_builder
        .lock()
        .await
        .write_lexicon()
        .await
        .map_err(|e| format!("Error writing lexicon: {}", e))?;

    index_builder
        .lock()
        .await
        .write_article_lengths()
        .map_err(|e| format!("Error writing article lengths: {}", e))?;

    index_builder
        .lock()
        .await
        .update_all_inv_index_files()
        .map_err(|e| format!("Error updating inverted index files: {}", e))?;

    Ok(article_count)
}

async fn index_article(
    article: Article,
    index_path: String,
    index_builder: Arc<Mutex<IndexBuilder>>,
) -> Result<(), String> {
    snippet_engine::insert_article(&article, &index_path)
        .map_err(|e| format!("Error inserting article: {e}"))?;

    index_builder.lock().await.build_index(&article);

    Ok(())
}
