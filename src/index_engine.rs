use std::collections::HashMap;

use bzip2::read::MultiBzDecoder;
use xml::reader::{EventReader, XmlEvent};

enum Tag {
    Title,
    Id,
    Text,
    Other,
}

#[derive(Debug, Clone)]
pub struct Article {
    title: String,
    id: usize,
    text: String,
}

pub async fn build_index(
    wiki_dump_path: String,
    metadata_output_path: String,
) -> Result<usize, String> {
    let mut wiki_dump_file = std::fs::File::open(wiki_dump_path).unwrap();
    let mut reader = MultiBzDecoder::new(&mut wiki_dump_file);
    let parser = EventReader::new(&mut reader);

    let mut id_to_title = HashMap::<usize, String>::new();

    if let Err(err) = parse_dump(parser, &mut id_to_title).await {
        return Err(format!("Error parsing dump: {}", err));
    }

    // We should dump id_to_title to some metadata file here

    Ok(id_to_title.len())
}

async fn parse_dump(
    parser: EventReader<&mut MultiBzDecoder<&mut std::fs::File>>,
    id_to_title: &mut HashMap<usize, String>,
) -> Result<(), String> {
    let mut cur_tag = Tag::Other;
    let mut cur_doc = Article {
        title: String::new(),
        id: usize::MAX,
        text: String::new(),
    };

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
            Ok(XmlEvent::EndElement { name: _ }) => {
                cur_tag = Tag::Other;
            }
            Ok(XmlEvent::Characters(chars)) => {
                match cur_tag {
                    Tag::Title => {
                        cur_doc.title = chars;
                    }
                    Tag::Id => {
                        if cur_doc.id == usize::MAX {
                            cur_doc.id = chars.parse::<usize>().unwrap();
                            id_to_title.insert(cur_doc.id, cur_doc.title.clone());
                        }
                    }
                    Tag::Text => {
                        cur_doc.text = chars;
                        let doc = cur_doc.clone();

                        // Call Rankings Engine async
                        // Call Lexicon Engine async

                        if cur_doc.id == 10000 {
                            // Limit to first 10k articles for now... :/
                            break;
                        }
                        cur_doc.id = usize::MAX;
                    }
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return Err(format!("Failed to parse XML at index: {}", cur_doc.id));
            }
            _ => {}
        }
    }

    Ok(())
}
