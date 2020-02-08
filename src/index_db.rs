extern crate rusqlite;

use rusqlite::{Connection, Result};
use rusqlite::{params, NO_PARAMS};


#[derive(Debug)]
pub struct IndexRecord {
    pub checksum: String,
    pub name: String,
    pub path: String,
}


pub fn create() -> Result<()> {
    let conn = Connection::open("index.db")?;

    conn.execute(
        "create table if not exists index_records (
             checksum text primary key,
             name text not null,
             path text             
         )",
        NO_PARAMS,
    )?;

    Ok(())
}


pub fn insert(arr: &[IndexRecord]) -> Result<()> {
    let conn = Connection::open("index.db")?;

    for record in arr {
        conn.execute(
            "INSERT INTO index_records (checksum, name, path) values (?1, ?2, ?3)",
            params![record.checksum, record.name, record.path]
        )?;
    }

    Ok(())
}


pub fn select(name: String) -> Result<Vec<IndexRecord>> {
    let conn = Connection::open("index.db")?;
    let prepared_name = format!("%{}%", name);

    let mut stmt = conn.prepare(
        "SELECT i.checksum, i.name, i.path
         FROM index_records i
         WHERE i.name LIKE $1 ;"
    )?;

    let records = stmt.query_map(params![prepared_name], |row| {
        Ok(IndexRecord {
            checksum: row.get(0)?,
            name: row.get(1)?,
            path: row.get(2)?,
        })
    })?;

    let res = records.map(|r| r.unwrap()).collect::<Vec<IndexRecord>>();
    Ok(res)
}

