use anyhow::{Ok, Result};
use sha2::{Digest, Sha256};
use sqlite::ConnectionThreadSafe;

struct Generate{
    no: String,
    hash: String
}

pub struct Yum {
    pub done_populating: bool,
    pub location: String,
    connection: ConnectionThreadSafe,
    pub percentage: f64,
    generated: Vec<Generate>
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
    pub fn insert(&mut self)->Result<()>{
        let mut insert_query = "INSERT INTO nums (hash, no) VALUES ".to_string();
        let mut iter = self.generated.iter().peekable();
        while let Some(generate) = iter.next(){
            let mut val = format!("( \"{}\", {})", generate.hash, generate.no);
            if let Some(_) = iter.peek(){
                val.push_str(",");
            }else{
                val.push_str(";");
            }
            insert_query.push_str(&val);
        }

        self.connection.execute(insert_query)?;
        self.generated.clear();
        Ok(())
    }

    pub fn get(&self, num_hash: &str)-> Result<Option<String>>{
        let select_query = "SELECT * FROM nums WHERE hash = ?";

        let result = self.connection.prepare(select_query)?
            .into_iter().bind((1, num_hash))?
            .next();
        match result {
            Some(r) => Ok(Some(r?.read::<i64, _>("no").to_string())),
            None => Ok(None)
        }
    }

    pub fn add_generate(&mut self, no: String, hash: String){
        self.generated.push(Generate{
            no,
            hash
        })
    }

    pub fn generated_size_is(&self, bytes: usize)-> bool{
        let num_elements = self.generated.len();
        let element_size = std::mem::size_of::<Generate>();
        let total_bytes = num_elements * element_size;
        bytes < total_bytes
    }

    pub fn start_filling(&mut self)->Result<()>{
        //check if all fields filled
        let result: i64 = unsafe {
            // to break borrow checker to allow immutable borrow on a mutable borrow
            let myself: *mut Yum = self;
            let myself: &mut Yum = &mut *myself;
            let mut statement = myself.connection.prepare("SELECT COUNT(*) FROM nums")?;
            statement.next()?;
            statement.read(0)?
        };
        let mut done =  result as f64;

        for num in (254000000000.0 + done) as u64..=254999999999 {
            let mut hasher = Sha256::new();
            hasher.update(num.to_string());
            let num_hash: String = format!("{:x}",hasher.finalize());

            self.add_generate(num.to_string(), num_hash);
            if self.generated_size_is(1024*1024*20){
                //20mb
                self.insert()?;
            }
            done += 1.0;

            self.percentage = ( done  / 999999999.0) * 100.0 ;
        }

        if !self.generated.is_empty() {
            self.insert()?;
            self.percentage = 100.0;
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
        percentage: 0.0,
        generated: Vec::default()
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

        yum.add_generate(num.clone(), num_hash.clone());
        yum.insert().unwrap();
        let number_result: String = yum.get(&num_hash).unwrap().unwrap();

        assert_eq!(num, number_result);

        assert_eq!(number_result, "254712345689")
    }
}
