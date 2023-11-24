mod common;
mod index_engine;
mod query;

use clap::Parser;

use index_engine::index_engine::build_index;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    build_index: bool,
    #[arg(short, long)]
    search: Option<String>,
    #[arg(short, long)]
    wiki_dump_path: Option<String>,
    #[arg(short, long)]
    index_path: String,
    #[arg(short, long, default_value_t = 10)]
    num_max_results: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.build_index {
        let wiki_dump_path = args.wiki_dump_path;
        if wiki_dump_path.is_none() {
            println!("wiki-dump-path is required to build index");
            return;
        }
        let wiki_dump_path = wiki_dump_path.unwrap();

        let index_path = args.index_path.clone();

        match build_index(&wiki_dump_path, &index_path).await {
            Ok(num_articles) => {
                println!("Index built with {} articles", num_articles);
            }
            Err(err) => {
                println!("Error building index: {}", err);
            }
        }
    }

    if args.search.is_some() {
        let query = args.search.unwrap();
        let index_path = args.index_path.clone();
        let num_max_results = args.num_max_results;

        match query::get_query_results(&query, num_max_results, &index_path) {
            Ok(query_results) => {
                println!("Query results for \"{}\":\n", query);
                for query_result in query_results {
                    println!(
                        "Title: {}\nArticle ID: {}\nScore: {}\nSnippet: {}\n",
                        query_result.title,
                        query_result.article_id,
                        query_result.score,
                        query_result.snippet
                    );
                }
            }
            Err(err) => {
                println!("Error querying index: {}", err);
            }
        }
    }
}
