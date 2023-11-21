use std::collections::HashMap;

use bzip2::read::MultiBzDecoder;
use xml::reader::{EventReader, XmlEvent};

use super::{
    index_builder::{self, IndexBuilder},
    snippet_engine,
};

enum Tag {
    Title,
    Id,
    Text,
    Other,
}

#[derive(Debug, Clone)]
pub struct Article {
    pub id: usize,
    pub title: String,
    pub text: String,
}

impl Article {
    fn new() -> Self {
        Article {
            title: String::new(),
            id: usize::MAX,
            text: String::new(),
        }
    }

    fn parsed_id(&self) -> bool {
        self.id != usize::MAX
    }
}

pub async fn build_index(wiki_dump_path: &String, output_path: &String) -> Result<usize, String> {
    let mut wiki_dump_file = std::fs::File::open(wiki_dump_path).unwrap();
    let mut reader = MultiBzDecoder::new(&mut wiki_dump_file);
    let parser = EventReader::new(&mut reader);

    let mut id_to_title = HashMap::<usize, String>::new();

    if let Err(err) = parse_dump(parser, &mut id_to_title, output_path).await {
        return Err(format!("Error parsing dump: {}", err));
    }

    // We should dump id_to_title to some metadata file here

    Ok(id_to_title.len())
}

async fn parse_dump(
    parser: EventReader<&mut MultiBzDecoder<&mut std::fs::File>>,
    id_to_title: &mut HashMap<usize, String>,
    output_path: &String,
) -> Result<(), String> {
    let mut cur_tag = Tag::Other;
    let mut cur_article = Article::new();
    let mut index_builder = index_builder::IndexBuilder::new();

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
                    if let Err(e) =
                        index_article(&cur_article, output_path, &mut index_builder).await
                    {
                        return Err(format!("Error indexing article {}: {}", cur_article.id, e));
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
                        cur_article.id = chars.parse::<usize>().unwrap();
                        id_to_title.insert(cur_article.id, cur_article.title.clone());
                    }
                }
                Tag::Text => {
                    cur_article.text = chars;
                }
                _ => {}
            },
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return Err(format!("Failed to parse XML at index: {}", cur_article.id));
            }
            _ => {}
        }
    }

    Ok(())
}

async fn index_article(
    article: &Article,
    output_path: &String,
    index_builder: &mut IndexBuilder,
) -> Result<(), String> {
    if let Err(e) = snippet_engine::snippet_engine(article, output_path).await {
        return Err(format!("Error writing snippet: {}", e));
    }

    if let Err(e) = index_builder.build_index(article) {
        return Err(format!("Error building index: {}", e));
    }

    Ok(())
}
