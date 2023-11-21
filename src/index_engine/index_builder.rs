use std::collections::HashMap;

use rust_stemmers::Stemmer;

use super::index_engine::Article;

#[derive(Default)]
pub struct IndexBuilder {
    cur_token_id: usize,
    id_to_token: HashMap<usize, String>,
    token_to_id: HashMap<String, usize>,
    inv_index: HashMap<usize, HashMap<usize, usize>>,
}

impl IndexBuilder {
    pub fn new() -> Self {
        IndexBuilder::default()
    }

    pub fn build_index(&mut self, article: &Article) -> Result<(), String> {
        let tokens = tokenize(&article.text);
        let token_ids = self.get_token_ids(&tokens);
        let word_counts = self.count_words(&token_ids);
        self.update_inv_index(article.id, &word_counts);
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
            let token_inv_index = self.inv_index.entry(*token_id).or_insert(HashMap::new());
            token_inv_index.insert(article_id, *count);
        }
    }
}

fn tokenize(text: &String) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let text = text.to_lowercase();
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
