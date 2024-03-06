use anyhow::{Ok, Result};
use sqlite::{Connection, Value};

pub struct Yum {
    pub done_populating: bool,
    pub location: String,
    connection: Connection,
    pub percentage: f32
}

impl Yum {
    pub fn insert(&mut self, num_hash: &str, num: &str)->Result<()>{
        let insert_query = "INSERT INTO nums VALUES( ?, ?)";
        let mut statement = self.connection.prepare(insert_query)?;
        statement.bind::<&[(_, Value)]>(&[(1,num_hash.into()),(2,num.into())])?;
        let _ = statement.next();

        Ok(())
    }

    pub fn get(&self, num_hash: &str)-> Result<String>{
        let select_query = "SELECT * FROM nums WHERE hash = ?";

        let result = self.connection.prepare(select_query)?
            .into_iter().bind((1, num_hash))?
            .next().unwrap()?.read::<i64, _>("no").to_string();
        Ok(result)
    }

    pub fn start_filling()->Result<()>{
        Ok(())
    }
}

pub fn init(location: Option<&str>) -> Result<Yum> {
    let location = if let Some(x) = location {
        x.to_string()
    } else {
        ":memory:".to_string()
    };

    let connection = sqlite::open(location.clone())?;

    let yum = Yum {
        location,
        done_populating: false,
        connection,
        percentage: 0.0
    };

    let create_table = "CREATE TABLE IF NOT EXISTS nums (hash TEXT PRIMARY KEY, no INTEGER)";

    yum.connection.execute(create_table)?;

    Ok(yum)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;

    #[test]
    fn init_db_memory() {
        init(None).unwrap();
    }

    #[test]
    fn init_db_disk() {
        let f = tempfile::NamedTempFile::new().unwrap();
        init(f.path().to_str()).unwrap();
    }

    #[test]
    fn insert_and_check() {
        let num = "254712345689";
        let mut yum = init(None).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(num);
        let num_hash: String = format!("{:x}",hasher.finalize());
        yum.insert(&num_hash, &num).unwrap();
        let number_result: String = yum.get(&num_hash).unwrap();

        assert_eq!(number_result, num)
    }
}