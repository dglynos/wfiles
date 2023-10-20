use clap::{App, AppSettings};

mod stash;
mod db;
mod fs;
mod hasher;
mod dup;
mod dup_prune;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    let params = App::new("wfiles")
        .version(VERSION)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(stash::args_config())
        .subcommand(dup::args_config());

    let matches = params.get_matches();
    match matches.subcommand() {
        ("stash", Some(sub_m)) => {
            let stash_op = stash::StashOperation::from_args(&sub_m);
            stash_op.do_operation();
        },
        ("dup", Some(sub_m)) => {
            let dup_op = dup::DupOperation::from_args(&sub_m);
            dup_op.do_operation();
        },
        _ => { println!("{}", matches.usage()); std::process::exit(1); },
    }
}
