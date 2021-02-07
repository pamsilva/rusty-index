use std::collections::HashMap;

extern crate rusqlite;
use rusqlite::{params, NO_PARAMS};
use rusqlite::{Connection, Result};

extern crate chrono;
use chrono::{DateTime, SecondsFormat, Utc};

#[derive(Debug)]
pub struct IndexRecord {
    pub id: u32,
    pub checksum: String,
    pub name: String,
    pub path: String,
    pub modified: DateTime<Utc>,
}

pub trait IndexStorage {
    fn create(&self) -> Result<()>;
    fn insert(&self, arr: &Vec<IndexRecord>) -> Result<()>;
    fn select(&self, name: String) -> Result<Vec<IndexRecord>>;
    fn fetch_sorted(&self) -> Result<Vec<IndexRecord>>;
    fn fetch_indexed(&self) -> Result<HashMap<String, IndexRecord>>;
}

pub struct SQLite3 {
    pub conn: Connection,
}

pub fn initalise_db(file_name: &String) -> Result<SQLite3> {
    Ok(SQLite3 {
        conn: Connection::open(file_name.as_str())?,
    })
}


impl IndexStorage for SQLite3 {
    fn create(&self) -> Result<()> {
        self.conn.execute(
            "create table if not exists index_records (
             id integer primary key autoincrement,
             checksum text not null,
             name text not null,
             path text,
             modified text)",
            NO_PARAMS,
        )?;

        Ok(())
    }

    fn insert(&self, arr: &Vec<IndexRecord>) -> Result<()> {
        for record in arr {
            self.conn.execute(
                "INSERT INTO index_records (checksum, name, path, modified) values (?1, ?2, ?3, ?4)",
                params![record.checksum, record.name, record.path, record.modified.to_rfc3339_opts(SecondsFormat::Millis, true)]
            )?;
        }

        Ok(())
    }

    fn select(&self, name: String) -> Result<Vec<IndexRecord>> {
        let prepared_name = format!("%{}%", name);

        let mut stmt = self.conn.prepare(
            "SELECT i.id, i.checksum, i.name, i.path, i.modified
             FROM index_records i
             WHERE i.name LIKE $1 ;",
        )?;

        let records = stmt.query_map(params![prepared_name], |row| {
	    let str_modifeid: String = row.get(4)?;
	    Ok(IndexRecord {
                id: row.get(0)?,
                checksum: row.get(1)?,
                name: row.get(2)?,
                path: row.get(3)?,
		modified: DateTime::parse_from_rfc3339(str_modifeid.as_str()).expect("Failed to parse date from db").into(),
            })
        })?;

        let res = records.map(|r| r.unwrap()).collect::<Vec<IndexRecord>>();
        Ok(res)
    }

    fn fetch_sorted(&self) -> Result<Vec<IndexRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT i.id, i.checksum, i.name, i.path, i.modified
             FROM index_records i
             ORDER BY i.path, i.name;",
        )?;

        let records = stmt.query_map(NO_PARAMS, |row| {
	    let str_modifeid: String = row.get(4)?;
	    Ok(IndexRecord {
                id: row.get(0)?,
                checksum: row.get(1)?,
                name: row.get(2)?,
                path: row.get(3)?,
		modified: DateTime::parse_from_rfc3339(str_modifeid.as_str()).expect("Failed to parse date from db").into(),
            })
        })?;

        let res = records.map(|r| r.unwrap()).collect::<Vec<IndexRecord>>();
        Ok(res)
    }

    fn fetch_indexed(&self) -> Result<HashMap<String, IndexRecord>> {
	let mut stmt = self.conn.prepare(
            "SELECT i.id, i.checksum, i.name, i.path, i.modified
             FROM index_records i
             ORDER BY i.path, i.name;",
        )?;

        let records = stmt.query_map(NO_PARAMS, |row| {
	    let str_modifeid: String = row.get(4)?;
	    Ok(IndexRecord {
                id: row.get(0)?,
                checksum: row.get(1)?,
                name: row.get(2)?,
                path: row.get(3)?,
		modified: DateTime::parse_from_rfc3339(str_modifeid.as_str()).expect("Failed to parse date from db").into(),
            })
        })?;

	let mut res = HashMap::new();
	for _record in records {
	    let clean_record = _record.unwrap();
	    let path = clean_record.path.clone();
	    
	    match res.insert(path.clone(), clean_record) {
		None => continue,
		_ => println!("Repeated file in database: {}", path),
	    }
	}

	return Ok(res);
    }
}
