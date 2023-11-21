use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};

use super::index_engine::Article;

pub async fn snippet_engine(article: &Article, output_path: &String) -> io::Result<()> {
    let snippet_file_path = format!("{}/{}.txt", output_path, article.id);
    let mut snippet_file = File::create(snippet_file_path).await?;
    let text = article.text.as_bytes();
    let mut pos = 0;

    while pos < text.len() {
        let bytes_written = snippet_file.write(&text[pos..]).await?;
        pos += bytes_written;
    }

    Ok(())
}
