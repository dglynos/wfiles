use std::fs::File;
use std::io::prelude::*;
use vmap::Map;
use libc::{posix_fadvise, c_int, off_t};
use std::os::unix::io::AsRawFd;
use walkdir::{WalkDir,DirEntry};
use std::path::Path;
use std::io::Error;

use crate::hasher::ByteHasher;

const POSIX_FADV_SEQUENTIAL : c_int = 2;

pub struct FileVisitor<F>
    where F: FnMut(&DirEntry)
{
    pub dry_run: bool,
    pub verbose: bool,
    pub file_processor : F
}

impl<F> FileVisitor<F>
    where F: FnMut(&DirEntry)
{
    pub fn traverse<T: AsRef<Path>>(&mut self, 
                                    topdirs: &Vec<T>, 
                                    n_items_hint: usize) -> usize
    {
        let fp = &mut self.file_processor;
        let mut n_entries : usize = 0;
        for dir in topdirs {
            for entry in WalkDir::new(dir) {
                match entry {
                   Ok(_entry) => if _entry.file_type().is_file() { 
                        n_entries += 1;
                        if !self.dry_run { 
                            if self.verbose {
                                eprint!("> processing {} ({}/{})",
                                    _entry.path().display(),
                                    n_entries,
                                    n_items_hint);
                            }
                            fp(&_entry);
                            if self.verbose {
                                eprint!("\x1b[2K\r");
                            }
                        } 
                   },
                   Err(_err) => eprint!("Failed to walk dir entry ({})\n",
                                        _err)
                }
            }
        }
        return n_entries;
    }    
}

pub struct FileHasher<B: ByteHasher>
{
    pub hasher : Box<B>,
    pub buf : Vec<u8>,
    pub force_read : bool
}

impl <B> FileHasher<B> 
    where B: ByteHasher, 
{
	pub fn new(_hasher: B, bufsz : u64, force_read: bool) -> Self {
	    return Self {   hasher: Box::new(_hasher),
                        buf : vec![0; bufsz as usize],
                        force_read : force_read}
	}
	
	pub fn hash_filehandle(&mut self, f: &mut File, file_size: u64) -> String {
        let mut gotta_try_read = true;

        if cfg!(unix) {
            unsafe {
                let adv : c_int;
                adv = posix_fadvise(f.as_raw_fd(), 
                              0 as off_t, 
                              0 as off_t,
                              POSIX_FADV_SEQUENTIAL);
                if adv !=0 {
                    eprintln!("error: POSIX_FADV_SEQUENTIAL was not applied");
                }
            }
        }

        if !self.force_read { // let's try mmap
            let try_mmap = Map::with_options().map(f);
            if try_mmap.is_ok() {
                gotta_try_read = false;
                self.hasher.update(&try_mmap.unwrap().as_ref());
            }
        }

        if gotta_try_read == true {
            let buf = &mut self.buf;
    	    let mut n : usize;

	        loop {
                n = f.read(buf).expect("error reading file!");
                if n != 0 {
                    self.hasher.update(&mut buf[..n]);
                    if n == file_size as usize { break };
                } else {
                    break;
                }
            }
        }

        self.hasher.finish();
        self.hasher.digest()
	}

    pub fn hash_dbentry(&mut self, path: &str, fname : &str, file_size: u64) -> Result<String, Error>  
    {
        let full_path : String = format!("{}{}{}", 
                    path,
                    std::path::MAIN_SEPARATOR, 
                    fname);
        
        let mut f = File::open(&full_path)?;
        Ok(self.hash_filehandle(&mut f, file_size))
	}


	
}


