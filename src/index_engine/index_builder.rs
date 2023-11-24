use std::{collections::HashMap, io::Write, path::Path};

use crate::common::{tokenize, Article, MAX_POSTINGS_LIST_DIRECTORY_SIZE, MAX_POSTINGS_LIST_SIZE};

pub struct IndexBuilder {
    cur_token_id: usize,
    id_to_token: HashMap<usize, String>,
    token_to_id: HashMap<String, usize>,
    index_path: String,
    inv_index: HashMap<usize, Vec<(usize, usize)>>,
    article_lengths: HashMap<usize, usize>,
}

impl IndexBuilder {
    pub fn new(index_path: &String) -> Result<Self, String> {
        std::fs::create_dir_all(&index_path)
            .map_err(|e| format!("Error creating index directory: {e}"))?;

        Ok(IndexBuilder {
            cur_token_id: 0,
            id_to_token: HashMap::new(),
            token_to_id: HashMap::new(),
            index_path: index_path.clone(),
            inv_index: HashMap::new(),
            article_lengths: HashMap::new(),
        })
    }

    pub fn build_index(&mut self, article: &Article) {
        let tokens = tokenize(&article.text);
        let token_ids = self.get_token_ids(&tokens);
        let word_counts = self.count_words(&token_ids);
        self.update_inv_index(article.id, &word_counts);
        self.article_lengths.insert(article.id, tokens.len());
    }

    pub async fn write_lexicon(&self) -> Result<(), String> {
        let lexicon_path = Path::new(&self.index_path).join("lexicon.bin");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&lexicon_path)
            .map_err(|e| format!("Error opening file: {e}"))?;

        // Serde lexicon to json
        // serde_json::to_writer(&mut file, &self.id_to_token)
        //     .map_err(|e| format!("Error writing to lexicon file: {e}"))?;

        bincode::serialize_into(&mut file, &self.id_to_token)
            .map_err(|e| format!("Error writing to lexicon file: {e}"))?;

        Ok(())
    }

    pub fn update_all_inv_index_files(&mut self) -> Result<(), String> {
        let token_ids = self.inv_index.keys().copied().collect::<Vec<usize>>(); // Create a copy of the token IDs
        for token_id in token_ids {
            self.update_inv_index_file(token_id)
                .map_err(|e| format!("Error updating inverted index file: {e}"))?;
        }
        Ok(())
    }

    pub fn write_article_lengths(&self) -> Result<(), String> {
        let article_lengths_path = Path::new(&self.index_path).join("article_lengths.bin");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&article_lengths_path)
            .map_err(|e| format!("Error opening file: {e}"))?;

        bincode::serialize_into(&mut file, &self.article_lengths)
            .map_err(|e| format!("Error writing to article lengths file: {e}"))?;

        Ok(())
    }

    fn get_token_ids(&mut self, tokens: &Vec<String>) -> Vec<usize> {
        let mut token_ids = Vec::new();
        for token in tokens {
            token_ids.push(self.get_token_id(token));
        }
        token_ids
    }

    fn get_token_id(&mut self, token: &String) -> usize {
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

    fn count_words(&self, token_ids: &Vec<usize>) -> HashMap<usize, usize> {
        let mut word_counts = HashMap::<usize, usize>::new();
        for token_id in token_ids {
            let count = word_counts.entry(*token_id).or_insert(0);
            *count += 1;
        }
        word_counts
    }

    fn update_inv_index(&mut self, article_id: usize, word_counts: &HashMap<usize, usize>) {
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

    fn update_inv_index_file(&mut self, token_id: usize) -> Result<(), String> {
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
