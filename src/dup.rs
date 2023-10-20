use clap::{App, Arg, SubCommand, ArgMatches};
use crate::db;
use crate::dup_prune;

pub struct DupOperation<'a> {
    dbfile: &'a str,
    verbose: bool,
    to_prune: bool,
}

impl<'a> DupOperation<'a> {
    pub fn from_args(sub_m: &'a ArgMatches<'a>) -> Self {
        let mut _dbfile : &str = db::DEFAULT_DB;
 
        if let Some(_db) = sub_m.value_of("db") {
            _dbfile = _db;
        }

        return DupOperation {
            dbfile: _dbfile,
            verbose: sub_m.is_present("verbose"),
            to_prune: sub_m.is_present("prune"),
        };
    }

    pub fn do_operation(&self) {
        let store = db::DataBase::for_reading(self.dbfile);
        let mut dup_state = db::IdentifyDupsStatement::new(&store);
        let dups = dup_state.get_dups();
        let mut how_much_would_be_freed : u64 = 0;
        for dup in &dups {  
            how_much_would_be_freed += (dup.num_dups-1) * dup.size;
            if self.verbose {
                println!("{}", dup.one_path);
            } else {
                print!("{}", dup.one_path);
            }
            for other_path in &dup.other_paths {
                if self.verbose {
                    println!("`-- {}", other_path);
                } else {
                    print!(",{}", other_path);
                }
            }
            if !self.verbose { println!(); }
        }
        if self.verbose {
            println!("{} sets of duplicate files found", dups.len());
            println!("{} bytes would be freed by removing duplicates",
                how_much_would_be_freed);
        }

        if !self.to_prune {
            return;
        }

        let rules = dup_prune::collect_dup_path_rules(&dups);
        for (_, rule) in rules.iter() {
            println!("{}", rule);
        }
    }
}

pub fn args_config<'a, 'b>() -> App<'a, 'b> {
    return SubCommand::with_name("dup")
            .about("Identifies duplicates in stash file")
            .usage("wfiles dup [-v] [-d <FILE>] [-p]")
            .arg(Arg::with_name("db")
                 .short("d")
                 .value_name("FILE")
                 .default_value("stash.db")
                 .help("Sets stash file"))
            .arg(Arg::with_name("verbose")
                .short("v")
                .help("Verbose mode"))
            .arg(Arg::with_name("prune")
                .short("p")
                .help("Prunes duplicates according to strategy"));
}

