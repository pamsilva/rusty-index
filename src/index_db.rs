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


pub trait IndexStorage {
    fn create(&self) -> Result<()>;
    fn insert(&self, arr: &Vec<IndexRecord>) -> Result<()> ;
    fn select(&self, name: String) -> Result<Vec<IndexRecord>>;
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
             path text             
         )",
            NO_PARAMS,
        )?;

        Ok(())
    }

    fn insert(&self, arr: &Vec<IndexRecord>) -> Result<()> {
        for record in arr {
            self.conn.execute(
                "INSERT INTO index_records (checksum, name, path) values (?1, ?2, ?3)",
                params![record.checksum, record.name, record.path]
            )?;
        }

        Ok(())
    }

    fn select(&self, name: String) -> Result<Vec<IndexRecord>> {
        let prepared_name = format!("%{}%", name);

        let mut stmt = self.conn.prepare(
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
}


// #cfg[test()]
// mod test {
//     use supper::*;

//     #[test]
//     fn test_db_interaction() {
        
//     }
// }
