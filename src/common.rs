use std::collections::HashMap;

use rust_stemmers::Stemmer;
use serde::{Deserialize, Serialize};

pub const MAX_ARTICLE_DIR_SIZE: usize = 1000;
pub const MAX_POSTINGS_LIST_SIZE: usize = 10000;
pub const MAX_POSTINGS_LIST_DIRECTORY_SIZE: usize = 1000;
pub const B: f64 = 0.75;
pub const K1: f64 = 1.2;
pub const K2: f64 = 100.0;
pub const SNIPPET_OFFSET: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Article {
    pub id: usize,
    pub title: String,
    pub text: String,
}

impl Article {
    pub fn new() -> Self {
        Article {
            title: String::new(),
            id: usize::MAX,
            text: String::new(),
        }
    }

    pub fn parsed_id(&self) -> bool {
        self.id != usize::MAX
    }
}

pub struct QueryResult {
    pub article_id: usize,
    pub title: String,
    pub snippet: String,
    pub score: f64,
}

pub fn tokenize(text: &String) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let text = text.to_lowercase().replace(|c: char| !c.is_ascii(), ""); // non-ascii chars were making things wonky
    let stemmer = Stemmer::create(rust_stemmers::Algorithm::English);

    for (i, c) in text.chars().enumerate() {
        if !c.is_alphanumeric() {
            if start != i {
                tokens.push(stemmer.stem(&text[start..i]).to_string());
            }
            start = i + 1;
        }
    }

    if start != text.len() {
        tokens.push(stemmer.stem(&text[start..text.len()]).to_string());
    }

    tokens
}

pub fn tokenize_with_positions(text: &String) -> HashMap<String, Vec<usize>> {
    let mut tokens = HashMap::new();
    let mut start = 0;
    let text = text.to_lowercase().replace(|c: char| !c.is_ascii(), ""); // non-ascii chars were making things wonky
    let stemmer = Stemmer::create(rust_stemmers::Algorithm::English);

    for (i, c) in text.chars().enumerate() {
        if !c.is_alphanumeric() {
            if start != i {
                let positions = tokens
                    .entry(stemmer.stem(&text[start..i]).to_string())
                    .or_insert(Vec::new());
                positions.push(start);
            }
            start = i + 1;
        }
    }

    if start != text.len() {
        let positions = tokens
            .entry(stemmer.stem(&text[start..text.len()]).to_string())
            .or_insert(Vec::new());
        positions.push(start);
    }

    tokens
}
