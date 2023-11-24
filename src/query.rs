use std::{
    collections::{BTreeMap, HashMap},
    io::BufRead,
};

use crate::common::{
    tokenize, tokenize_with_positions, Article, QueryResult, B, K1, K2, MAX_ARTICLE_DIR_SIZE,
    MAX_POSTINGS_LIST_DIRECTORY_SIZE, SNIPPET_OFFSET,
};

pub fn get_query_results(
    query: &String,
    num_max_results: usize,
    index_path: &String,
) -> Result<Vec<QueryResult>, String> {
    let index_path = std::path::Path::new(index_path);
    let mut scores: Vec<(usize, f64)> = Vec::new();
    let mut query_results = Vec::new();

    let article_lengths_path = index_path.join("article_lengths.bin");
    let article_lengths_file = std::fs::File::open(article_lengths_path)
        .map_err(|e| format!("Failed to open article_lengths.bin: {e}"))?;
    let article_lengths: HashMap<usize, usize> = bincode::deserialize_from(article_lengths_file)
        .map_err(|e| format!("Failed to parse article_lengths.bin: {e}"))?;

    let lexicon_path = index_path.join("lexicon.bin");
    let lexicon_file = std::fs::File::open(lexicon_path)
        .map_err(|e| format!("Failed to open lexicon.bin file: {e}"))?;
    let lexicon: HashMap<usize, String> = bincode::deserialize_from(lexicon_file)
        .map_err(|e| format!("Failed to parse lexicon.bin file: {e}"))?;
    let reverse_lexicon: HashMap<String, usize> = lexicon
        .iter()
        .map(|(k, v)| (v.clone(), k.clone()))
        .collect();

    let mut query_token_ids = Vec::new();
    for token in &tokenize(query) {
        match reverse_lexicon.get(token) {
            Some(token_id) => {
                query_token_ids.push(token_id.clone());
            }
            None => {
                continue;
            }
        }
    }
    let query_token_freqs = query_token_ids
        .iter()
        .fold(BTreeMap::new(), |mut acc, token_id| {
            let count = acc.entry(*token_id).or_insert(0);
            *count += 1;
            acc
        });

    let postings_lists = get_postings_lists(&query_token_ids, index_path)?;

    let average_article_length =
        article_lengths.values().sum::<usize>() as f64 / article_lengths.len() as f64;
    let num_articles = article_lengths.len();

    for article_id in article_lengths.keys() {
        match calculate_bm25(
            *article_id,
            *article_lengths.get(article_id).unwrap(),
            &query_token_freqs,
            average_article_length,
            num_articles,
            &postings_lists,
        ) {
            Ok(score) => {
                scores.push((*article_id, score));
            }
            Err(e) => {
                eprintln!(
                    "Failed to calculate BM25 score for article {}: {}",
                    article_id, e
                );
                continue;
            }
        }
    }

    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    for (article_id, score) in &scores[..num_max_results] {
        let article = match get_article(*article_id, index_path) {
            Ok(article) => article,
            Err(e) => {
                eprintln!("Failed to get article {}: {}", article_id, e);
                continue;
            }
        };
        let article_snippet = match get_article_snippet(&article.text, &query_token_freqs, &lexicon)
        {
            Ok(snippet) => snippet,
            Err(e) => {
                eprintln!("Failed to get snippet for article {}: {}", article_id, e);
                continue;
            }
        };
        query_results.push(QueryResult {
            article_id: *article_id,
            title: article.title,
            snippet: article_snippet,
            score: *score,
        });
    }

    Ok(query_results)
}

fn get_postings_lists(
    query_token_ids: &Vec<usize>,
    index_path: &std::path::Path,
) -> Result<HashMap<usize, HashMap<usize, usize>>, String> {
    let mut postings_lists: HashMap<usize, HashMap<usize, usize>> = HashMap::new();

    for token_id in query_token_ids {
        let postings_list_path = index_path
            .join("inv_index")
            .join(format!("{}", token_id / MAX_POSTINGS_LIST_DIRECTORY_SIZE))
            .join(format!("{token_id}.txt"));
        let postings_list_file = std::fs::File::open(postings_list_path)
            .map_err(|e| format!("Failed to open postings_list file: {e}"))?;
        let postings_list = read_postings_list_file(&postings_list_file)?;

        postings_lists.insert(*token_id, postings_list);
    }

    Ok(postings_lists)
}

fn read_postings_list_file(
    postings_list_file: &std::fs::File,
) -> Result<HashMap<usize, usize>, String> {
    let mut postings_list: HashMap<usize, usize> = HashMap::new();

    let mut reader = std::io::BufReader::new(postings_list_file);
    let mut line = String::new();
    while reader.read_line(&mut line).unwrap() > 0 {
        let mut line_split = line.split_whitespace();
        let article_id = line_split
            .next()
            .ok_or(format!("Failed to parse postings_list file"))?
            .parse::<usize>()
            .map_err(|e| format!("Failed to parse postings_list file: {e}"))?;
        let frequency = line_split
            .next()
            .ok_or(format!("Failed to parse postings_list file"))?
            .parse::<usize>()
            .map_err(|e| format!("Failed to parse postings_list file: {e}"))?;

        postings_list.insert(article_id, frequency);

        line.clear();
    }

    Ok(postings_list)
}

fn calculate_bm25(
    article_id: usize,
    article_length: usize,
    query_token_freqs: &BTreeMap<usize, usize>,
    average_article_length: f64,
    num_articles: usize,
    postings_lists: &HashMap<usize, HashMap<usize, usize>>,
) -> Result<f64, String> {
    let mut score = 0.0;

    for (query_token_id, query_token_freq) in query_token_freqs {
        let postings_list = postings_lists.get(query_token_id).ok_or(format!(
            "Failed to get postings_list for token_id {}",
            query_token_id
        ))?;

        let frequency = match postings_list.get(&article_id) {
            Some(frequency) => *frequency as f64,
            None => {
                continue;
            }
        };

        let k = K1 * ((1.0 - B) + B * article_length as f64 / average_article_length);
        let tf = (K1 + 1.0) * frequency / (k + frequency);
        let qf = (K2 + 1.0) * *query_token_freq as f64 / (K2 + *query_token_freq as f64);
        let idf = ((num_articles as f64 - postings_list.len() as f64 + 0.5)
            / (postings_list.len() as f64 + 0.5)
            + 1.0)
            .ln();
        score += tf * qf * idf;
    }

    Ok(score)
}

fn get_article(article_id: usize, index_path: &std::path::Path) -> Result<Article, String> {
    let article_path = index_path
        .join("articles")
        .join(format!("{}", article_id / MAX_ARTICLE_DIR_SIZE))
        .join(format!("article_{}.json", article_id));
    let article_file = std::fs::File::open(article_path)
        .map_err(|e| format!("Failed to open article file: {e}"))?;
    let article: Article = serde_json::from_reader(article_file)
        .map_err(|e| format!("Failed to parse article file: {e}"))?;

    Ok(article)
}

fn get_article_snippet(
    article_text: &String,
    query_token_freqs: &BTreeMap<usize, usize>,
    lexicon: &HashMap<usize, String>,
) -> Result<String, String> {
    let article_text = article_text.replace(|c: char| !c.is_ascii(), "");
    let tokens_with_positions = tokenize_with_positions(&article_text);

    for (query_token_id, _) in query_token_freqs.iter().rev() {
        let token = match lexicon.get(query_token_id) {
            Some(token) => token,
            None => {
                continue;
            }
        };
        if let Some(positions) = tokens_with_positions.get(token) {
            if let Some(position) = positions.first() {
                let start = if *position > SNIPPET_OFFSET {
                    *position - SNIPPET_OFFSET
                } else {
                    0
                };
                let end = if *position + SNIPPET_OFFSET > article_text.len() {
                    article_text.len()
                } else {
                    *position + SNIPPET_OFFSET
                };
                let snippet = format!("...{}...", &article_text[start..end]).replace("\n", " ");
                return Ok(snippet);
            }
        }
    }

    Err(format!("Failed to find snippet"))
}
