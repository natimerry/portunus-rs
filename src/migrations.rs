use crate::database::{Database, MigrationEntry};
use chrono::{DateTime, NaiveDateTime, Utc};
use colored::Colorize;
use regex::Regex;
use std::{
    collections::BTreeMap,
    fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};
use tabled::{
    Table,
    Tabled,
    settings::{Settings, Style},
};
use walkdir::WalkDir;

#[derive(Tabled)]
struct MigrationTablePrint {
    /* since this can only be fetched from the db instance this cannot be null */
    id: i64,
    filename: String,
    hash: String,
    timestamp: DateTime<Utc>,
}

impl From<&MigrationEntry> for MigrationTablePrint {
    fn from(value: &MigrationEntry) -> Self {
        MigrationTablePrint {
            id: value.id.unwrap(),
            filename: value.filename.clone(),
            hash: value.hash.clone(),
            timestamp: value.timestamp.unwrap(),
        }
    }
}
pub fn get_migration_status(db: &Database) {
    let table_config = Settings::default().with(Style::psql());
    let migs: Vec<MigrationTablePrint> = db
        .get_migrations()
        .iter()
        .map(|x| MigrationTablePrint::from(x))
        .collect::<Vec<MigrationTablePrint>>();
    let table = Table::new(migs).with(table_config).to_string();

    println!("{}", table);
}

pub fn create_new_migration(migration_dir: &Path, migration_name: &str) {
    let re_number = Regex::new(r"^(\d+)[._-]").unwrap();
    let re_timestamp = Regex::new(r"^(\d{14})[._-]").unwrap();

    let mut max_number = 0;
    let mut latest_timestamp: Option<NaiveDateTime> = None;
    let mut mode = None;

    if let Ok(entries) = migration_dir.read_dir() {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("sql") {
                continue;
            }

            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                if let Some(caps) = re_number.captures(file_name) {
                    if let Ok(num) = caps[1].parse::<u32>() {
                        max_number = max_number.max(num);
                        mode = Some("number");
                    }
                } else if let Some(caps) = re_timestamp.captures(file_name) {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(&caps[1], "%Y%m%d%H%M%S") {
                        latest_timestamp = Some(latest_timestamp.map_or(dt, |prev| prev.max(dt)));
                        mode = Some("timestamp");
                    }
                }
            }
        }
    }

    let new_file_name = match mode {
        Some("number") => {
            let new_number = max_number + 1;
            format!("{new_number}_{migration_name}.sql")
        }
        Some("timestamp") | None => {
            let timestamp = Utc::now().format("%Y%m%d%H%M%S");
            format!("{timestamp}_{migration_name}.sql")
        }
        _ => unreachable!(),
    };

    let new_file_path = migration_dir.join(&new_file_name);
    let mut file = File::create(&new_file_path).expect("Failed to create migration file");
    writeln!(file, "-- Migration: {new_file_name}").expect("Failed to write to migration file");

    println!("Created migration: {}", new_file_path.display());
}

pub fn run_migration(
    db: &mut Database,
    migration_dir: &Path,
    dry_run: bool,
    force: bool,
) {
    let files: Vec<PathBuf> = WalkDir::new(migration_dir)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter_map(|entry| entry.ok()) // skip errored entries
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| entry.path().extension().and_then(|e| e.to_str()) == Some("sql"))
        .map(|entry| entry.into_path())
        .collect();

    let existing_entries = db
        .get_migrations()
        .iter()
        .map(|m| m.clone())
        .collect::<Vec<MigrationEntry>>();

    let mut transaction = db.start_transaction().expect("Unable to start transaction");

    let existing_map: BTreeMap<String, MigrationEntry> = existing_entries
        .into_iter()
        .map(|m| (m.filename.clone(), m))
        .collect();

    let mut current_map = BTreeMap::new();

    for file in &files {
        let sql = fs::read_to_string(file).expect("Failed to read sql file");
        let entry = MigrationEntry::new(file); // computes hash internally
        current_map.insert(entry.filename.clone(), (entry, sql));
    }
    for (filename, existing) in &existing_map {
        match current_map.get(filename) {
            Some((current, _)) => {
                if current.hash != existing.hash {
                    eprintln!(
                        "{}\n    → {}",
                        "Migration has changed after being applied.".red().bold(),
                        filename
                    );

                    if force {
                        eprintln!(
                            "{}",
                            "... ignoring as user has asked me to force the migrations"
                                .yellow()
                                .bold()
                        );
                        continue;
                    } else {
                        eprintln!("{}", "... refusing to continue.".red().bold());
                        transaction
                            .rollback()
                            .expect("Failed to rollback migration");
                        return;
                    }
                }
            }
            None => {
                eprintln!(
                    "{}\n    → {}",
                    "Migration was previously applied but is now missing."
                        .red()
                        .bold(),
                    filename.yellow()
                );

                if force {
                    eprintln!(
                        "{}",
                        "... ignoring as user has asked me to force the migrations"
                            .yellow()
                            .bold()
                    );
                    continue;
                } else {
                    eprintln!("{}", "... refusing to continue.".red().bold());
                    transaction
                        .rollback()
                        .expect("Failed to rollback migration");
                    return;
                }
            }
        }
    }

    let total = current_map.len();
    for (idx, (filename, (entry, sql))) in current_map.iter().enumerate() {
        if existing_map.contains_key(filename.as_str()) {
            continue;
        }


        let id = Database::run_new_migration(&mut transaction, &entry, &sql);
        if let Err(e) = id {
            eprintln!(
                "{} {}\n{} {}",
                "✗ Failed to run migration:".red().bold(),
                filename.yellow(),
                "→ Error:".bright_red(),
                e
            );
            transaction
                .rollback()
                .expect("Failed to rollback migration");
            return;
        }
        let id = id.unwrap();
        println!(
            "[{}] {} (ID: {})",
            format!("{}/{}", idx + 1, total).truecolor(128, 128, 128),
            filename.green(),
            id.to_string().yellow()
        );
    }

    if dry_run {
        transaction.rollback().ok();
        return;
    }
    println!("Migration completed");
    transaction.commit().ok();
}


