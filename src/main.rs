mod database;
mod migrations;

use std::path::PathBuf;
use crate::database::{Database};
use clap::{Args, CommandFactory, Parser, Subcommand};
use dotenv::dotenv;
use crate::migrations::{create_new_migration, get_migration_status, run_migration};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    db_url: Option<String>,

    #[arg(short, long, default_value = "./migrations")]
    migrations_dir: PathBuf,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Subcommand to execute
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Apply pending migrations
    Migrate(MigrationArgs),
    Status,
    New(NewMigrationArgs),
    Reset,
}

#[derive(Args, Debug)]
struct MigrationArgs {
    #[arg(short = 'n', long)]
    dry_run: bool,

    #[arg(short, long)]
    force: bool,

}
#[derive(Args, Debug)]
struct NewMigrationArgs {
    /// Name for the new migration
    name: String,
}

fn main() {
    let _env = dotenv();

    let args = Cli::parse();

    if std::env::var("DATABASE_URL").is_err() && args.db_url.is_none() {
        eprintln!("No DATABASE_URL set in environment. Pass --db-url");
        let _ = Cli::command().print_help();
    }
    let db_url = if let Some(url) = args.db_url {
        url
    } else {
        std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable not set")
    };

    if args.migrations_dir.exists() && !args.migrations_dir.is_dir() {
        eprintln!("Migrations directory does not exist or is not a directory");
        std::process::exit(1);
    }
    if !args.migrations_dir.exists(){
        std::fs::create_dir_all(&args.migrations_dir).unwrap();
    }
    let mut db = Database::init(&db_url).expect("Failed to initialize database");

    match args.command {
        Some(Commands::Migrate(mig_args)) => {
            run_migration(&mut db,&args.migrations_dir,mig_args.dry_run,mig_args.force);
        },
        Some(Commands::Status) => {
            get_migration_status(&db);
        }
        Some(Commands::Reset) => {
            db.reset(&db_url).expect("Failed to reset database");
        }
        Some(Commands::New(mig_args)) => {
            create_new_migration(&args.migrations_dir,&mig_args.name);
        }
        _ => {
            let _ = Cli::command().print_help();
            return;
        }
    }
}
