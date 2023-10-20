use rusqlite::{Connection, Transaction, Result, CachedStatement, OpenFlags};
use rusqlite::params;
use rusqlite::ToSql;
use std::path::Path;
use std::path::PathBuf;
use std::fs;
use std::fmt::Debug;
use std::str::FromStr;
use crate::fs::FileHasher;
use crate::hasher::SlowHasher;

pub const DEFAULT_DB : &str = "stash.db";
const DB_VERSION : u16 = 0x0100;

pub struct FileMetadata<'a> {
    pub path : &'a Path, 
    pub fasthash : Option<String>,
    pub slowhash : Option<String>,
    pub medium : &'a str,
    pub size : u64
}

pub struct DataBase {
    conn: Connection,
    force_sha512: bool,
    only_slowhash: bool
}

impl DataBase {
    fn is_compatible(file_version: u16) -> bool {
        (file_version & 0xff00) == (DB_VERSION & 0xff00)
    }

    fn is_initialized(conn: &Connection) -> bool {
        let res : Result<String> = conn.query_row("SELECT * from config", 
                                               [], |r| r.get(1), );
        res.is_ok()
    }

    fn config_db_sql(key: &str, value: &dyn std::fmt::Display) -> String 
    {
        format!("INSERT INTO config (key, value) VALUES('{}', '{}')", key, value)
    }

    fn init_db(conn: &Connection, version: u16, force_sha512: bool, only_slowhash: bool) {
        let config_version_stmt = Self::config_db_sql("version", &version);
        let config_force_sha512_stmt = Self::config_db_sql("force_sha512", &force_sha512);
        let config_only_slowhash_stmt = Self::config_db_sql("only_slowhash", &only_slowhash);

        let sql = [
            "CREATE TABLE config
            (config_id INTEGER NOT NULL,
             key TEXT NOT NULL UNIQUE,
             value TEXT,
             PRIMARY KEY (config_id))",

            "CREATE TABLE media
            (medium_id INTEGER NOT NULL,
            medium text NOT NULL UNIQUE,
            medium_comment text,
            PRIMARY KEY (medium_id))",

            "CREATE TABLE files
            (medium_id INTEGER NOT NULL, 
            path text NOT NULL, 
            fname text NOT NULL, 
            fasthash text,
            slowhash text,
            size INTEGER NOT NULL,
            CONSTRAINT files_id PRIMARY KEY (medium_id, path, fname),
            FOREIGN KEY(medium_id) REFERENCES media(medium_id))",

            "CREATE INDEX idx_fasthash ON files (fasthash)",

            "CREATE INDEX idx_slowhash ON files (slowhash)",

            "CREATE INDEX idx_size ON files (size)",

            "CREATE INDEX idx_fname ON files (fname)",

            "CREATE INDEX idx_medium ON media (medium)",

            "CREATE INDEX idx_config ON config (key)",

            &config_version_stmt,

            &config_force_sha512_stmt,

            &config_only_slowhash_stmt,

        ].join(";");

        conn.execute_batch(&sql).expect("SQL failed during db init");
    }

    fn get_config_value<T>(conn: &Connection, key: &str) -> T 
    where T: FromStr, <T as FromStr>::Err: Debug
    {
        let sql = format!("SELECT value from config where key='{}'", &key);
        let error = format!("did not find {} configuration in stash file", &key);

        let value: String = conn.query_row(&sql, [], |row| row.get(0)).expect(&error);
        value.parse::<T>().unwrap()
    }

    pub fn new<P: AsRef<Path>>(_path: P, _force_db_overwrite: bool, force_sha512: bool, only_slowhash: bool) -> DataBase
    {
        let db_file_exists = fs::metadata(&_path).is_ok();

        if db_file_exists && _force_db_overwrite {
            fs::remove_file(&_path)
                .expect("error in removing stash file");
        }

        let path : &Path = _path.as_ref();
        let conn = Connection::open_with_flags(path, 
                        OpenFlags::SQLITE_OPEN_READ_WRITE | 
                        OpenFlags::SQLITE_OPEN_CREATE).
                        expect("error opening/creating stash file");

        if !Self::is_initialized(&conn) {
            Self::init_db(&conn, DB_VERSION, force_sha512, only_slowhash);
        } else {
            if !Self::is_compatible(Self::get_config_value(&conn, "version")) {
                panic!("stash file cannot be processed by this version of wfiles");
            }
            if force_sha512 != Self::get_config_value(&conn, "force_sha512") {
                panic!("stash file was generated under different force_sha512 setting (see -s option)");
            }
            if only_slowhash != Self::get_config_value(&conn, "only_slowhash") {
                panic!("stash file was generated under different only_slowhash setting (see -l option)");
            }
        }

        DataBase { conn, force_sha512, only_slowhash }
   }

   pub fn for_reading<P: AsRef<Path>>(_path: P) -> DataBase
   {
        let path : &Path = _path.as_ref();
        let conn = Connection::open_with_flags(path, 
                        OpenFlags::SQLITE_OPEN_READ_ONLY).  
                        expect("error opening stash file");

        let force_sha512 = Self::get_config_value(&conn, "force_sha512");
        let only_slowhash = Self::get_config_value(&conn, "only_slowhash");

        if !Self::is_initialized(&conn) {
            panic!("non initialized stash file found");
        }

        if !Self::is_compatible(Self::get_config_value(&conn, "version")) {
            panic!("stash file cannot be processed by this version of wfiles");
        }

        DataBase { conn, force_sha512, only_slowhash }
   }        
}

pub struct DBTransaction<'conn> {
    trans: Transaction<'conn>
}

impl<'conn> DBTransaction<'conn> {
    pub fn new(db: &mut DataBase) -> DBTransaction {
        let trans = db.conn.transaction()
            .expect("error receiving transaction handler");
        DBTransaction { trans }
    }

    pub fn commit(self) {
        self.trans.commit().expect("error during commit");
    }
}

pub struct CheckCollisionStatement<'conn> {
    check_collision : CachedStatement<'conn>,
    update_slowhash : CachedStatement<'conn>,
}

impl<'conn> CheckCollisionStatement<'conn> {
    pub fn new<'d>(dt: &'d DBTransaction) -> CheckCollisionStatement<'d> {
        let cc_state = dt.trans.prepare_cached(
            "SELECT rowid, path, fname, size from files where fasthash = ?")
            .expect("error compiling check collision statement");
        let us_state = dt.trans.prepare_cached("UPDATE files set slowhash=? where rowid=?")
            .expect("error compiling update of slowhash statement");
        CheckCollisionStatement { check_collision: cc_state, update_slowhash: us_state}
    }

    fn trigger_slowhashing(&mut self, fh: &mut FileHasher<SlowHasher>, rowid: u64, path: &str, fname : &str, fsize: u64) {
        let digest = fh.hash_dbentry(path, fname, fsize).expect("error while accessing file for triggered slowhashing");
        self.update_slowhash.execute(params![&digest, rowid as i64]).expect("error while creating slowhash in trigger_slowhashing()");
    }

    pub fn collision(&mut self, fh: &mut FileHasher<SlowHasher>, fasthash: &str) -> bool {
        let mut rows = self.check_collision.query([fasthash])
            .expect("error when performing query for quickhash collision");

        if let Ok(Some(row)) = rows.next() {
            let rowid : u64 = row.get_unwrap::<usize, i64>(0) as u64;
            let path : String = row.get_unwrap::<usize, String>(1);
            let fname : String = row.get_unwrap::<usize, String>(2);
            let fsize : u64 = row.get_unwrap::<usize, i64>(3) as u64;
            drop(rows);
            self.trigger_slowhashing(fh, rowid, &path, &fname, fsize);
            return true
        }

        return false

    }
}

pub struct FileInsertStatement<'conn> {
    file_insert : CachedStatement<'conn>
}

impl<'conn> Drop for FileInsertStatement<'conn> {
    fn drop(&mut self) {}
}

impl<'conn> FileInsertStatement<'conn> {
    pub fn new<'c>(dt: &'c DBTransaction) -> FileInsertStatement<'c> {
        let fi_state = dt.trans.prepare_cached(
            "INSERT into files
            (medium_id, path, fname, fasthash, slowhash, size) 
            values ((select medium_id from media where medium = ? ), 
                    ?, ?, ?, ?, ?)")
            .expect("error compiling file insertion statement");
        FileInsertStatement { file_insert : fi_state }
    }

    pub fn add_file(&mut self, fm: FileMetadata) {
        self.file_insert.execute(
            params![fm.medium, fm.path.parent().unwrap().to_str().unwrap(), 
            fm.path.file_name().unwrap().to_str().unwrap(), 
            fm.fasthash.to_sql().unwrap(), fm.slowhash.to_sql().unwrap(), fm.size as i64]).unwrap_or_else(|_| 
                panic!("INSERT for file {:?}", fm.path));
    }
}

pub struct MediaInsertStatement<'conn> {
    media_insert : CachedStatement<'conn>
}

impl<'conn> Drop for MediaInsertStatement<'conn> {
    fn drop(&mut self) {}
}

impl<'conn> MediaInsertStatement<'conn> {
    pub fn new<'d>(dt: &'d DBTransaction) -> MediaInsertStatement<'d> {
        let mi_state = dt.trans.prepare_cached(
            "INSERT OR IGNORE INTO media (medium, medium_comment) 
             VALUES (?, ?)")
            .expect("error compiling media insertion statement");
        MediaInsertStatement { media_insert: mi_state}
    }

    pub fn try_add_medium(&mut self, medium: &str, descr: &str) {
        self.media_insert.execute(
            params![medium, descr]).expect("adding medium entry");
    }

}

pub struct DupFile {
    pub one_path: String,
    pub other_paths: Vec<String>,
    pub hash: String,
    pub num_dups: u64,
    pub size: u64
}

impl DupFile {
    pub fn format_minimal(&self, indexed: bool) -> String {
        let mut st = String::new();
        let mut idx : usize = 1;
        if indexed {
            st.push_str(&format!("{}. ", idx));
        }
        st.push_str(&self.one_path);
        idx += 1;
        for st_other in &self.other_paths {
            st.push('\n');
            if indexed {
                 st.push_str(&format!("{}. ", idx));
            }
            st.push_str(&st_other);
            idx += 1;
        }
        st
    }

    pub fn path_sig(&self) -> String
    {
        let one_path_dir = PathBuf::from(&self.one_path).parent()
            .unwrap_or(&PathBuf::from(&format!("{}",std::path::MAIN_SEPARATOR)))
.to_path_buf();
        let mut vec = vec![one_path_dir];
        for p in &self.other_paths {
            let other_path_dir = PathBuf::from(&p).parent()
            .unwrap_or(&PathBuf::from(&format!("{}",std::path::MAIN_SEPARATOR)))
.to_path_buf();
            vec.push(other_path_dir);
        }
        vec.sort();
        vec.dedup();

        let mut new_vec : Vec<&str> = Vec::new();
        for _n in &vec {
            new_vec.push(_n.to_str().unwrap());
        }
        new_vec.join(",")
    }
                    
}


pub struct IdentifyDupsStatement<'conn> {
    identify_dups: CachedStatement<'conn>
}

impl<'conn> IdentifyDupsStatement<'conn> {
    pub fn new(db: &DataBase) -> IdentifyDupsStatement {
        let id_state = db.conn.prepare_cached(
            "SELECT files.path, files.fname, files.slowhash, 
             T2.P, files.size from files join (select slowhash,COUNT(*) AS P 
             FROM files WHERE slowhash is NOT NULL GROUP BY slowhash HAVING COUNT(*) > 1 ORDER BY slowhash) 
             T2 ON files.slowhash = T2.slowhash;")
            .expect("error compiling lazy dup query statement");
        IdentifyDupsStatement { identify_dups: id_state }
    }

    pub fn get_dups(&mut self) -> Vec<DupFile> {
        let mut v : Vec<DupFile> = Vec::new();
        let mut rows = self.identify_dups.query([])
            .expect("error executing dup query");

        let mut start_of_new_duplicate_set = true;
        let mut remaining_dups : u64 = 0;

        while let Some(row) = rows.next()
            .expect("could not retrieve next row") 
        {
            if start_of_new_duplicate_set {
               let num_dups : u64 = row.get_unwrap::<usize,i64>(3) as u64;
               let hash : String = row.get_unwrap::<usize,String>(2);
               let size : u64 = row.get_unwrap::<usize, i64>(4) as u64;
               let v2: Vec<String> = Vec::new();
               let one_path : String = format!("{}{}{}", 
                    row.get_unwrap::<usize,String>(0),
                    std::path::MAIN_SEPARATOR, 
                    row.get_unwrap::<usize,String>(1));
               v.push(DupFile { one_path,
                                other_paths: v2,
                                hash,
                                num_dups, 
                                size });
               remaining_dups = num_dups - 1;
               start_of_new_duplicate_set = false;
            } else {
                let last = v.last_mut().unwrap();
                let one_path : String = format!("{}{}{}", 
                    row.get_unwrap::<usize,String>(0),
                    std::path::MAIN_SEPARATOR,
                    row.get_unwrap::<usize,String>(1));
                last.other_paths.push(one_path);
                remaining_dups -= 1;
                if remaining_dups == 0 {
                    start_of_new_duplicate_set = true;
                }
            }
        }
        return v;
    }
}

