extern crate rusqlite;

use rusqlite::{Connection, Result};
use rusqlite::{params, NO_PARAMS};


#[derive(Debug)]
pub struct IndexRecord {
    pub id: u32,
    pub checksum: String,
    pub name: String,
    pub path: String,
}


pub fn create() -> Result<()> {
    let conn = Connection::open("index.db")?;

    conn.execute(
        "create table if not exists index_records (
             id integer primary key autoincrement,
             checksum text not null,
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
        "SELECT i.id, i.checksum, i.name, i.path
         FROM index_records i
         WHERE i.name LIKE $1 ;"
    )?;

    let records = stmt.query_map(params![prepared_name], |row| {
        Ok(IndexRecord {
            id: row.get(0)?,
            checksum: row.get(1)?,
            name: row.get(2)?,
            path: row.get(3)?,
        })
    })?;

    let res = records.map(|r| r.unwrap()).collect::<Vec<IndexRecord>>();
    Ok(res)
}


// #cfg[test()]
// mod test {
//     use supper::*;

//     #[test]
//     fn test_db_interaction() {
        
//     }
// }
