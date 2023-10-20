use indexmap::map::IndexMap;
use std::path::PathBuf;
use std::io;
use std::fmt;
use std::collections::HashSet;
use regex::Regex;
use crate::db::DupFile;

#[derive(Copy, Clone)]
#[allow(non_camel_case_types)]
enum KeepStrategy { 
    KEEP_AS_IS,
    KEEP_THIS_OF_THESE(usize),
    KEEP_THIS_OF_ANY(usize),
    KEEP_ANY_ONE,
    KEEP_OLDEST,
    KEEP_NEWEST,
}

impl fmt::Display for KeepStrategy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let string : &str  = match self {
            KeepStrategy::KEEP_AS_IS => "Keep all versions",
            KeepStrategy::KEEP_THIS_OF_THESE(_) => "Keep specific version from specific versions (requires index parameter)",
            KeepStrategy::KEEP_THIS_OF_ANY(_) => "Keep specific version from any versions (requires index parameter)",
            KeepStrategy::KEEP_ANY_ONE => "Keep a random version",
            KeepStrategy::KEEP_OLDEST => "Keep oldest version",
            KeepStrategy::KEEP_NEWEST => "Keep latest version",
        };
        write!(f, "{}", string)
    }
}

impl KeepStrategy {
    fn from_str(s : &str, max_idx: usize, default: Option<KeepStrategy>) 
        -> Result<KeepStrategy, &'static str>
    {
        if let Some(d) = default {
            if let KeepStrategy::KEEP_THIS_OF_THESE(_) = d {
                return Err("Cannot use \"keep this of these\" rule as default rule");
            }
            if let KeepStrategy::KEEP_THIS_OF_ANY(_) = d {
                return Err("Cannot use \"keep this of any\" rule as default rule");
            }
        }

        let re_keep_as_is = Regex::new("^a").unwrap();
        let re_keep_this_of_these = Regex::new("^b ([0-9]+)").unwrap();
        let re_keep_this_of_any = Regex::new("^c ([0-9]+)").unwrap();
        let re_keep_any_one = Regex::new("^d").unwrap();
        let re_keep_oldest = Regex::new("^e").unwrap();
        let re_keep_newest = Regex::new("^f").unwrap();
 
        if re_keep_as_is.is_match(s) {
            return Ok(KeepStrategy::KEEP_AS_IS);
        } else if re_keep_this_of_these.is_match(s) {
            let idx = re_keep_this_of_these.captures(s).unwrap().get(1).unwrap().as_str().parse::<usize>().unwrap();
            if idx > max_idx {
                return Err("Invalid index for Keep This of These strategy");
            }
            return Ok(KeepStrategy::KEEP_THIS_OF_THESE(idx));
        } else if re_keep_this_of_any.is_match(s) {
            let idx = re_keep_this_of_any.captures(s).unwrap().get(1).unwrap().as_str().parse::<usize>().unwrap();
            if idx > max_idx {
                return Err("Invalid index for Keep This Of Any strategy");
            }
            return Ok(KeepStrategy::KEEP_THIS_OF_ANY(idx));
        } else if re_keep_any_one.is_match(s) {
            return Ok(KeepStrategy::KEEP_ANY_ONE);
        } else if re_keep_oldest.is_match(s) {
            return Ok(KeepStrategy::KEEP_OLDEST);
        } else if re_keep_newest.is_match(s) {
            return Ok(KeepStrategy::KEEP_NEWEST);
        }

        return match default {
            None => Err("No default keep strategy set"),
            Some(t) => Ok(t),
        }; 
    }
}

pub struct DirBasedPruneRule {
    verdict: KeepStrategy,
    paths: Vec<PathBuf>,
}

// a little helper for the DirBasedPruneRule formatter
fn list_and_highlight(paths : &Vec<PathBuf>, highlighted : Option<usize>)
        -> String
{
    let mut s = String::new();
    for i in 1..(paths.len()+1) {
        if highlighted.is_some() && (highlighted.unwrap() == i) {
            s.push_str("(*) ");
        } else {
            s.push_str("+-- ");
        }
        s.push_str(paths[i-1].to_str().unwrap());
        s.push_str("\n");
    }
    return s;
}

impl fmt::Display for DirBasedPruneRule {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s : String = match self.verdict {
            KeepStrategy::KEEP_AS_IS => format!("--- Keep as is\n{}", list_and_highlight(&self.paths, None)),
            KeepStrategy::KEEP_THIS_OF_THESE(i) => format!("--- Keep marked (*) of these\n{}", list_and_highlight(&self.paths, Some(i))),
            KeepStrategy::KEEP_THIS_OF_ANY(i) => format!("--- Keep marked (*) of any\n{}", list_and_highlight(&self.paths, Some(i))),
            KeepStrategy::KEEP_ANY_ONE => format!("--- Keep one randomly\n{}", list_and_highlight(&self.paths, None)),
            KeepStrategy::KEEP_OLDEST => format!("--- Keep oldest version\n{}", list_and_highlight(&self.paths, None)),
            KeepStrategy::KEEP_NEWEST => format!("--- Keep latest version\n{}", list_and_highlight(&self.paths, None)),
        };
        write!(f, "{}", s)
    }
}


// TODO FileBasedPruneRule

fn pick_dups_for_rules<'a>(dups: &'a Vec<DupFile>) -> Vec<&'a DupFile>
{
    let mut patterns = HashSet::new();
    let mut uniq_vec : Vec<&DupFile> = Vec::new();
    for d in dups {
        let one_path_dir = PathBuf::from(&d.one_path).parent()
            .unwrap_or(&PathBuf::from(&format!("{}",std::path::MAIN_SEPARATOR))).to_path_buf();
        let mut vec = vec![one_path_dir];
        for p in &d.other_paths {
            let other_path_dir = PathBuf::from(&p).parent()
            .unwrap_or(&PathBuf::from(&format!("{}",std::path::MAIN_SEPARATOR))).to_path_buf();
            vec.push(other_path_dir);
        }
        vec.sort();
        vec.dedup();

        if vec.len() == 1 { // this will get sorted later by a file-based rule
            continue;
        }

        let mut new_vec : Vec<&str> = Vec::new();
        for _n in &vec {
            new_vec.push(_n.to_str().unwrap());
        }
        let key = new_vec.join(",");

        if patterns.contains(&key) {
            continue;
        }

        patterns.insert(key);
        uniq_vec.push(&d);
    }
    uniq_vec
}

pub fn collect_dup_path_rules(dups: &Vec<DupFile>) -> IndexMap<String, DirBasedPruneRule>
{
    let mut patterns : IndexMap<String, DirBasedPruneRule> = IndexMap::new();
    let cases = pick_dups_for_rules(dups);
    let mut idx : usize = 1;
    for c in &cases {
        let one_path_dir = PathBuf::from(&c.one_path).parent()
            .unwrap_or(&PathBuf::from(&format!("{}",std::path::MAIN_SEPARATOR))).to_path_buf();
        let mut vec = vec![one_path_dir];
        for p in &c.other_paths {
            let other_path_dir = PathBuf::from(&p).parent()
            .unwrap_or(&PathBuf::from(&format!("{}",std::path::MAIN_SEPARATOR))).to_path_buf();
            vec.push(other_path_dir);
        }
        vec.sort();
        vec.dedup();

        let mut new_vec : Vec<&str> = Vec::new();
        for _n in &vec {
            new_vec.push(_n.to_str().unwrap());
        }
        let key = new_vec.join(",");

        println!("How should we handle this? [{} of {} decisions]\n{}", 
                 idx, cases.len(), c.format_minimal(true));
        println!("a. {}", KeepStrategy::KEEP_AS_IS);
        println!("b. {}", KeepStrategy::KEEP_THIS_OF_THESE(1));
        println!("c. {}", KeepStrategy::KEEP_THIS_OF_ANY(1));
        println!("d. {}", KeepStrategy::KEEP_ANY_ONE);
        println!("e. {}", KeepStrategy::KEEP_OLDEST);
        println!("f. {}", KeepStrategy::KEEP_NEWEST);

        let mut choice = String::new();
        io::stdin().read_line(&mut choice).unwrap();

        let choice_type = KeepStrategy::from_str(&choice, vec.len(), 
            Some(KeepStrategy::KEEP_AS_IS)).unwrap();

        patterns.insert(key, DirBasedPruneRule 
                { verdict: choice_type, paths: vec });
        idx += 1;
    }
    return patterns;
}


