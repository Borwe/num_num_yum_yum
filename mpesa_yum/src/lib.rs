use anyhow::{Ok, Result};
use sha2::{Digest, Sha256};
use sqlite::{ConnectionThreadSafe, Value};

pub struct Yum {
    pub done_populating: bool,
    pub location: String,
    connection: ConnectionThreadSafe,
    pub percentage: u64
}

#[derive(Copy, Clone)]
pub struct YumMutUnsafePointer {
    pub ptr: *mut Yum
}

unsafe impl Send for YumMutUnsafePointer{}
unsafe impl Sync for YumMutUnsafePointer{}

impl YumMutUnsafePointer {
    pub fn get(&self)-> &mut Yum{
        unsafe {&mut *self.ptr}
    }
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

    pub fn start_filling(&mut self)->Result<()>{
        //check if all fields filled
        let result: i64 = unsafe {
            // to break borrow checker to allow immutable borrow on a mutable borrow
            let myself: *mut Yum = self;
            let myself: &mut Yum = &mut *myself;
            let mut statement = myself.connection.prepare("SELECT COUNT(*) FOM nums")?;
            statement.next()?;
            statement.read(0)?
        };

        let left: u64 = 254999999999 - result as u64;


        for num in (254000000000 + left) as u64..=254999999999 {
            println!("DOING {num}");
            let mut hasher = Sha256::new();
            hasher.update(num.to_string());
            let num_hash: String = format!("{:x}",hasher.finalize());

            self.insert(&num_hash, &num.to_string())?;

            self.percentage = ((254999999999 - num) / (254999999999 - 254000000000)) * 100 ;
        }
        Ok(())
    }
}

pub fn init(location: Option<&str>) -> Result<Yum> {
    let location = if let Some(x) = location {
        x.to_string()
    } else {
        ":memory:".to_string()
    };

    let connection = sqlite::Connection::open_thread_safe(location.clone())?;

    let yum = Yum {
        location,
        done_populating: false,
        connection,
        percentage: 0
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
        let num = (254712345689 as u64).to_string();
        let mut yum = init(None).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&num);
        let num_hash: String = format!("{:x}",hasher.finalize());
        assert_eq!(num_hash, "1e450f13cce411f78315ba2ed9bfc2e2d43b2234491b0713eeeeb6594c4df364");

        yum.insert(&num_hash, &num).unwrap();
        let number_result: String = yum.get(&num_hash).unwrap();

        assert_eq!(num, number_result);

        assert_eq!(number_result, "254712345689")
    }
}
