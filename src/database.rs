use chrono::{DateTime, Utc};
use postgres::{Client, NoTls, Transaction};
use sha2::{Digest, Sha256};
use std::{fmt::Debug, fs, path::Path, time::SystemTime};

#[derive(Debug, Clone)]
pub struct MigrationEntry {
    pub id: Option<i64>, // since this field is big serial we dont really care about this too much
    pub filename: String, // although this will be a path when reading, we only need file stem
    pub hash: String,
    pub timestamp: Option<DateTime<Utc>>,
}

impl MigrationEntry {
    pub fn new(filename: &Path) -> Self {
        let data = fs::read_to_string(&filename)
            .expect("Failed to read file")
            .trim()
            .to_string();
        let cleaned_sql = data
            .lines()
            .map(|line| {
                // Remove anything after `--` (SQL single-line comment)
                let no_comment = match line.find("--") {
                    Some(index) => &line[..index],
                    None => line,
                };
                no_comment.trim()
            })
            .filter(|line| !line.is_empty()) // Skip empty lines
            .collect::<Vec<_>>()
            .join("\n"); // Rejoin cleaned lines

        let mut hasher = Sha256::new();
        hasher.update(&cleaned_sql);
        let hash: String = format!("{:X}", hasher.finalize());

        let filename = filename
            .file_name()
            .expect("Failed to get filename")
            .to_str()
            .unwrap()
            .to_string();

        MigrationEntry {
            id: None,
            filename,
            hash,
            timestamp: None,
        }
    }
}
pub struct Database {
    conn: Client,
    migrations: Vec<MigrationEntry>,
}

impl Database {
    /// THIS DOES NOT WORK FOR SUPABASE, USE THE CLI TO RESET YOUR DB
    pub fn reset(mut self, db_url: &str) -> Result<(), postgres::Error> {
        let db_name = db_url.rsplitn(2, '/').next().unwrap();
        println!("Resetting database: {}", db_name);

        drop(self.conn);

        let base_url = db_url.rsplitn(2, '/').nth(1).unwrap();
        let sys_db_url = format!("{}/postgres", base_url);

        // Connect to the system database
        let mut sys_conn = postgres::Client::connect(&sys_db_url, postgres::NoTls)?;

        // Terminate other connections to the target DB
        let disconnect_query = format!(
            "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = '{}' AND pid <> pg_backend_pid();",
            db_name
        );
        sys_conn.execute(&disconnect_query, &[])?;

        // Drop and recreate the target database
        let drop_query = format!("DROP DATABASE IF EXISTS \"{}\";", db_name);
        sys_conn.execute(&drop_query, &[])?;

        let create_query = format!("CREATE DATABASE \"{}\";", db_name);
        sys_conn.execute(&create_query, &[])?;

        println!("✓ Database `{}` has been reset.", db_name);
        Ok(())
    }
    pub fn get_migrations(&self) -> &Vec<MigrationEntry> {
        &self.migrations
    }

    pub fn start_transaction(&mut self) -> Result<Transaction, postgres::Error> {
        self.conn.transaction()
    }
    fn create_schema(conn: &mut Client) -> Result<(), postgres::Error> {
        let query = "
            CREATE TABLE IF NOT EXISTS __portunus_migrations (
                id BIGSERIAL PRIMARY KEY,
                filename TEXT NOT NULL UNIQUE,
                hash TEXT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ";

        conn.execute(query, &[])?;

        let create_index_on_filename = "CREATE INDEX IF NOT EXISTS __idx_portunus_files ON __portunus_migrations (filename, hash)";
        conn.execute(create_index_on_filename, &[])?;
        Ok(())
    }

    pub fn fetch_existing_migrations(
        conn: &mut Client,
    ) -> Result<Vec<MigrationEntry>, postgres::Error> {
        let query = "SELECT id,filename,hash,timestamp FROM __portunus_migrations";
        let rows = conn.query(query, &[])?;

        let entry = rows
            .iter()
            .map(|row| {
                let id = row.get("id");
                let filename = row.get("filename");
                let hash = row.get("hash");
                let timestamp: SystemTime = row.get("timestamp");
                let timestamp: DateTime<Utc> = timestamp.into();
                MigrationEntry {
                    id,
                    filename,
                    hash,
                    timestamp: Some(timestamp),
                }
            })
            .collect::<Vec<MigrationEntry>>();
        Ok(entry)
    }
    pub fn init(db_url: &str) -> Result<Database, postgres::Error> {
        // Try connecting to the target DB
        match Client::connect(db_url, NoTls) {
            Ok(mut client) => {
                Self::create_schema(&mut client)?;
                let migrations = Self::fetch_existing_migrations(&mut client)?;
                Ok(Database {
                    conn: client,
                    migrations,
                })
            }
            Err(e) => {
                eprintln!("Error connecting to database: {}", e);

                // Check if the error is due to missing database
                if e.to_string().contains("does not exist") {
                    // Extract db name
                    let db_name = db_url.rsplitn(2, '/').next().unwrap();
                    let system_db_url = format!("{}/postgres", db_url.rsplitn(2, '/').nth(1).unwrap());

                    eprintln!("Attempting to create missing database `{}`...", db_name);

                    // Connect to the system database
                    let mut sys_client = Client::connect(&system_db_url, NoTls)?;

                    // Create the target database
                    sys_client.execute(
                        &format!("CREATE DATABASE \"{}\";", db_name),
                        &[],
                    )?;

                    drop(sys_client); // Just to be explicit

                    // Try connecting again
                    let mut client = Client::connect(db_url, NoTls)?;
                    Self::create_schema(&mut client)?;
                    let migrations = Self::fetch_existing_migrations(&mut client)?;
                    Ok(Database {
                        conn: client,
                        migrations,
                    })
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn run_new_migration(
        transaction: &mut Transaction,
        migration: &MigrationEntry,
        sql: &str,
    ) -> Result<i64, postgres::Error> {
        transaction.batch_execute(sql)?;
        let insert_query =
            "INSERT INTO __portunus_migrations (filename, hash) VALUES ($1, $2) RETURNING id";
        let row = transaction.query_one(insert_query, &[&migration.filename, &migration.hash])?;
        Ok(row.get("id"))
    }
}
