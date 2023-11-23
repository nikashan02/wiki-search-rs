use std::path::Path;

use super::index_engine::Article;

pub async fn insert_article(article: &Article, index_path: &String) -> Result<(), String> {
    let subdir = Path::new(index_path).join(format!("articles/{}", article.id / 1000));
    std::fs::create_dir_all(&subdir).map_err(|e| format!("Error creating directory: {e}"))?;

    let article_path = subdir.join(format!("article_{}.json", article.id.to_string()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&article_path)
        .map_err(|e| format!("Error opening file: {e}"))?;

    serde_json::to_writer(&mut file, &article)
        .map_err(|e| format!("Error writing to article file: {e}"))?;

    Ok(())
}
