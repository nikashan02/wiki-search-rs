use std::{collections::HashMap, io::Write, path::Path};

use rust_stemmers::Stemmer;

use super::index_engine::Article;

const MAX_POSTINGS_LIST_SIZE: usize = 10000;
const MAX_POSTINGS_LIST_DIRECTORY_SIZE: i32 = 1000;

pub struct IndexBuilder {
    cur_token_id: i32,
    id_to_token: HashMap<i32, String>,
    token_to_id: HashMap<String, i32>,
    index_path: String,
    inv_index: HashMap<i32, Vec<(i32, i32)>>,
    doc_lengths: HashMap<i32, i32>,
}

impl IndexBuilder {
    pub fn new(index_path: &String) -> Result<Self, String> {
        if std::path::Path::new(&index_path).exists() {
            std::fs::remove_dir_all(&index_path)
                .map_err(|e| format!("Error removing existing index directory: {e}"))?;
        }
        std::fs::create_dir_all(&index_path)
            .map_err(|e| format!("Error creating index directory: {e}"))?;

        Ok(IndexBuilder {
            cur_token_id: 0,
            id_to_token: HashMap::new(),
            token_to_id: HashMap::new(),
            index_path: index_path.clone(),
            inv_index: HashMap::new(),
            doc_lengths: HashMap::new(),
        })
    }

    pub fn build_index(&mut self, article: &Article) {
        let tokens = tokenize(&article.text);
        let token_ids = self.get_token_ids(&tokens);
        let word_counts = self.count_words(&token_ids);
        self.update_inv_index(article.id, &word_counts);
        self.doc_lengths.insert(article.id, tokens.len() as i32);
    }

    pub async fn write_lexicon(&self) -> Result<(), String> {
        let lexicon_path = Path::new(&self.index_path).join("lexicon.json");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&lexicon_path)
            .map_err(|e| format!("Error opening file: {e}"))?;

        // Serde lexicon to json
        serde_json::to_writer(&mut file, &self.id_to_token)
            .map_err(|e| format!("Error writing to lexicon file: {e}"))?;

        Ok(())
    }

    pub fn update_all_inv_index_files(&mut self) -> Result<(), String> {
        let token_ids = self.inv_index.keys().copied().collect::<Vec<i32>>(); // Create a copy of the token IDs
        for token_id in token_ids {
            self.update_inv_index_file(token_id)
                .map_err(|e| format!("Error updating inverted index file: {e}"))?;
        }
        Ok(())
    }

    pub fn write_doc_lengths(&self) -> Result<(), String> {
        let doc_lengths_path = Path::new(&self.index_path).join("doc_lengths.json");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&doc_lengths_path)
            .map_err(|e| format!("Error opening file: {e}"))?;

        serde_json::to_writer(&mut file, &self.doc_lengths)
            .map_err(|e| format!("Error writing to doc lengths file: {e}"))?;

        Ok(())
    }

    fn get_token_ids(&mut self, tokens: &Vec<String>) -> Vec<i32> {
        let mut token_ids = Vec::new();
        for token in tokens {
            token_ids.push(self.get_token_id(token));
        }
        token_ids
    }

    fn get_token_id(&mut self, token: &String) -> i32 {
        match self.token_to_id.get(token) {
            Some(token_id) => *token_id,
            None => {
                let token_id = self.cur_token_id;
                self.id_to_token.insert(token_id, token.clone());
                self.token_to_id.insert(token.clone(), token_id);
                self.cur_token_id += 1;
                token_id
            }
        }
    }

    fn count_words(&self, token_ids: &Vec<i32>) -> HashMap<i32, i32> {
        let mut word_counts = HashMap::<i32, i32>::new();
        for token_id in token_ids {
            let count = word_counts.entry(*token_id).or_insert(0);
            *count += 1;
        }
        word_counts
    }

    fn update_inv_index(&mut self, article_id: i32, word_counts: &HashMap<i32, i32>) {
        for (token_id, count) in word_counts {
            let token_postings_list = self.inv_index.entry(*token_id).or_insert(Vec::new());
            token_postings_list.push((article_id, *count));
            if token_postings_list.len() >= MAX_POSTINGS_LIST_SIZE {
                if let Err(e) = self.update_inv_index_file(*token_id) {
                    eprintln!("Error updating inverted index file: {}", e);
                }
            }
        }
    }

    fn update_inv_index_file(&mut self, token_id: i32) -> Result<(), String> {
        let subdir_path = Path::new(&self.index_path)
            .join("inv_index")
            .join(format!("{}", token_id / MAX_POSTINGS_LIST_DIRECTORY_SIZE));

        std::fs::create_dir_all(&subdir_path).map_err(|e| {
            format!(
                "Error creating subdirectory at {}: {e}",
                subdir_path.to_string_lossy()
            )
        })?;

        let token_postings_list = self
            .inv_index
            .get(&token_id)
            .ok_or(format!("Token ID {token_id} not found in inverted index"))?;
        let mut postings_list_string = token_postings_list
            .iter()
            .map(|(article_id, count)| format!("{} {}", article_id, count))
            .collect::<Vec<String>>()
            .join("\n");
        postings_list_string.push_str("\n");

        let postings_list_path = subdir_path.join(format!("{}.txt", token_id));
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&postings_list_path)
            .map_err(|e| format!("Error opening file: {e}"))?;
        file.write_all(postings_list_string.as_bytes())
            .map_err(|e| format!("Error writing to file: {e}"))?;

        // Clear postings list
        self.inv_index.insert(token_id, Vec::new());

        Ok(())
    }
}

fn tokenize(text: &String) -> Vec<String> {
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
