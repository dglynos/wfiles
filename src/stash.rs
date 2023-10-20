use clap::{App, AppSettings, Arg, SubCommand, ArgMatches};
use walkdir::DirEntry;
use std::fs::File;

use crate::db;
use crate::hasher;
use crate::fs;

const DEFAULT_PATH : &str = ".";
const DEFAULT_READ_BUFFER_SIZE : u64 = 8 * 1024 * 1024;

pub struct StashOperation<'a> {
    topdirs: Vec<&'a str>,
    dbfile: &'a str,
    bufsize: u64,
    verbose: bool,
    force_db_overwrite: bool,
    force_read: bool,
    force_sha512 : bool,
    only_slowhash : bool
}

impl<'a> StashOperation<'a> {
    pub fn from_args(sub_m: &'a ArgMatches<'a>) -> Self {
        let mut _topdirs : Vec<&str> = vec!(DEFAULT_PATH);
        let mut _dbfile : &str = db::DEFAULT_DB;
        let mut _bufsize : u64 = DEFAULT_READ_BUFFER_SIZE;
        let _force_db_overwrite : bool;
        let _verbose : bool;
        let _force_read : bool;
        let _force_sha512 : bool;
        let _only_slowhash : bool;

        if let Some(_p) = sub_m.values_of("path") {
            _topdirs = _p.collect();
        }

        if let Some(_db) = sub_m.value_of("db") {
            _dbfile = _db;
        }

        if let Some(_buf) = sub_m.value_of("buf") {
            _bufsize = _buf.parse::<u64>()
                .expect("error during parsing of buffer size to integer") 
                * 1024 * 1024;
        }

        _verbose = sub_m.is_present("verbose");
        _force_db_overwrite = sub_m.is_present("force_db_overwrite");
        _force_read = sub_m.is_present("force_read");
        _force_sha512 = sub_m.is_present("force_sha512");
        _only_slowhash = sub_m.is_present("only_slowhash");

        StashOperation {
            topdirs: _topdirs,
            dbfile: _dbfile,
            bufsize: _bufsize,
            verbose: _verbose,
            force_db_overwrite: _force_db_overwrite,
            force_read: _force_read,
            force_sha512 : _force_sha512,
            only_slowhash : _only_slowhash
        }
    }

    pub fn do_operation(&self) {
        let _medium = "filesystem";
        let medium_descr = "my computer";
	
	    let mut store = db::DataBase::new(self.dbfile, self.force_db_overwrite, self.force_sha512, self.only_slowhash);
	    let trans = db::DBTransaction::new(&mut store);
	    let mut media_statement = db::MediaInsertStatement::new(&trans);
	    let mut file_statement = db::FileInsertStatement::new(&trans);
        

        let mut quick_hasher = fs::FileHasher::new(hasher::QuickHasher::new(),
                                                      self.bufsize, 
                                                      self.force_read);
        let mut slow_hasher = match self.force_sha512 {
            false => fs::FileHasher::new(hasher::SlowHasher::MD5(), self.bufsize, self.force_read),
            true => fs::FileHasher::new(hasher::SlowHasher::SHA512(), self.bufsize, self.force_read)
        };

        let mut slow_hasher_lazy = match self.force_sha512 {
            false => fs::FileHasher::new(hasher::SlowHasher::MD5(), self.bufsize, self.force_read),
            true => fs::FileHasher::new(hasher::SlowHasher::SHA512(), self.bufsize, self.force_read)
        };

        /* CheckCollisionStatement needs a FileHasher<SlowHasher> to perform lazy slow hashing to the old entry
           when a new entry has a colliding "fast" hash */
        let mut check_collision = db::CheckCollisionStatement::new(&trans);

	    let do_nothing = |_: &DirEntry| {};
	
	    let hash_and_store = |dirent: &DirEntry| {
	        let _path = dirent.path();
            match File::open(_path) {
                Ok(_f) => {
                    let mut f = _f;
        	        let _size = f.metadata().unwrap().len();
                    let q_digest_str: Option<String>;
                    let s_digest_str: Option<String>;

                    q_digest_str = if self.only_slowhash {
                        None
                    } else {
                        Some(quick_hasher.hash_filehandle(&mut f, _size))
                    };

                    if self.only_slowhash || (!self.only_slowhash && check_collision.collision(&mut slow_hasher_lazy, q_digest_str.as_ref().unwrap().as_ref())) {
                        s_digest_str = Some(slow_hasher.hash_filehandle(&mut f, _size));
                    } else {
                        s_digest_str = None;
                    }
                    
        	        file_statement.add_file(db::FileMetadata{path: &_path, 
                                                                fasthash: q_digest_str,
                                                                slowhash: s_digest_str,
                                                                medium: _medium,
                                                                size: _size});
                },
                Err(_e) => eprint!("failed to open file ({})\n", _e)
            }
	    };
	
	    let mut fv_dry_run = fs::FileVisitor { 
	        dry_run: true,
	        verbose: false,
	        file_processor : do_nothing
	    };
	
	    let n_items = fv_dry_run.traverse(&self.topdirs, 0);
	
	    media_statement.try_add_medium(_medium, medium_descr);
	
	    let mut fv_actual = fs::FileVisitor {
	        dry_run: false,
	        verbose: self.verbose,
	        file_processor: hash_and_store
	    };
	
	    fv_actual.traverse(&self.topdirs, n_items);

        drop(media_statement);
        drop(file_statement);
        drop(check_collision);
	    trans.commit();
    }
}

pub fn args_config<'a, 'b>() -> App<'a, 'b> {
    return SubCommand::with_name("stash")
            .about("Creates a stash of recorded files")
            .usage("wfiles stash [-v] [-f] [-r] [-s] [-l] [-d <FILE>] [-b <SIZE>] <PATH> ...")
            .setting(AppSettings::TrailingVarArg)
            .arg(Arg::with_name("db")
                 .short("d")
                 .value_name("FILE")
                 .default_value("stash.db")
                 .help("Sets stash file"))
            .arg(Arg::with_name("force_db_overwrite")
                 .short("f")
                 .help("Forces the overwrite of an existing stash file"))
            .arg(Arg::with_name("force_read")
                 .short("r")
                 .help("Don't mmap(2), use read(2) instead"))
            .arg(Arg::with_name("force_sha512")
                .short("s")
                .help("Prefer SHA512 over MD5 for slow hashing"))
            .arg(Arg::with_name("only_slowhash")
                .short("l")
                .help("Disable quick hashing (and perform only slow hashing)"))
            .arg(Arg::with_name("verbose")
                .short("v")
                .help("Verbose mode"))
            .arg(Arg::with_name("buf")
                .short("b")
                .value_name("SIZE")
                .default_value("8")
                .help("Read-buffer size (in megabytes)"))
            .arg(Arg::with_name("path")
                .required(true)
//              .last(true) cannot use as it makes '--' mandatory :/
                .allow_hyphen_values(true)
                .multiple(true)
                .value_name("PATH")
                .help("Path(s) to examine"));
}

