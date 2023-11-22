pub mod index_engine;

use clap::Parser;

use deadpool_postgres::{ManagerConfig, RecyclingMethod};
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
    #[arg(long)]
    db_host: String,
    #[arg(long)]
    db_port: u16,
    #[arg(long)]
    db_user: String,
    #[arg(long)]
    db_password: String,
    #[arg(long)]
    db_name: String,
}

struct DatabaseArgs {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub name: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let db_config = DatabaseArgs {
        host: args.db_host,
        port: args.db_port,
        user: args.db_user,
        password: args.db_password,
        name: args.db_name,
    };

    let db_config = get_db_config(&db_config);

    if args.build_index {
        let wiki_dump_path = args.wiki_dump_path;
        if wiki_dump_path.is_none() {
            println!("wiki-dump-path is required to build index");
            return;
        }
        let wiki_dump_path = wiki_dump_path.unwrap();

        match build_index(&wiki_dump_path, &db_config).await {
            Ok(num_articles) => {
                println!("Index built with {} articles", num_articles);
            }
            Err(err) => {
                println!("Error building index: {}", err);
            }
        }
    }
}

fn get_db_config(db_args: &DatabaseArgs) -> deadpool_postgres::Config {
    let mut config = deadpool_postgres::Config::new();
    config.user = Some(db_args.user.clone());
    config.password = Some(db_args.password.clone());
    config.dbname = Some(db_args.name.clone());
    config.host = Some(db_args.host.clone());
    config.port = Some(db_args.port);
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    config
}
