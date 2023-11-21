pub mod index_engine;

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
    output_path: Option<String>,
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

        let output_path = args.output_path;
        if output_path.is_none() {
            println!("output-path is required to build index");
            return;
        }
        let output_path = output_path.unwrap();

        match build_index(&wiki_dump_path, &output_path).await {
            Ok(num_articles) => {
                println!("Index built with {} articles", num_articles);
            }
            Err(err) => {
                println!("Error building index: {}", err);
            }
        }
    }
}
