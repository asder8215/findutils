// Copyright 2017 Google Inc.
//
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

pub mod matchers;

use matchers::{Follow, WalkEntry};
use sharded_ringbuf::cs_srb::{CSShardedRingBuf};
use tokio::runtime::Handle;
use tokio::task::yield_now;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::error::Error;
use std::io::{BufWriter, Stdout, Write, stderr, stdout};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, RwLock};
use std::time::SystemTime;
use walkdir::WalkDir;
use std::iter::IntoIterator;

pub struct Config {
    same_file_system: bool,
    depth_first: bool,
    min_depth: usize,
    max_depth: usize,
    sorted_output: bool,
    help_requested: bool,
    version_requested: bool,
    today_start: bool,
    no_leaf_dirs: bool,
    follow: Follow,
    new_paths: Option<Vec<String>>,
    files0_argument: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            same_file_system: false,
            depth_first: false,
            min_depth: 0,
            max_depth: usize::MAX,
            sorted_output: false,
            help_requested: false,
            version_requested: false,
            today_start: false,
            // Directory information and traversal are done by walkdir,
            // and this configuration field will exist as
            // a compatibility item for GNU findutils.
            no_leaf_dirs: false,
            follow: Follow::Never,
            new_paths: None, // This option exclusively for -files0-from argument.
            files0_argument: None, //This option also is used for file0-from
        }
    }
}

/// Trait that encapsulates various dependencies (output, clocks, etc.) that we
/// might want to fake out for unit tests.
pub trait Dependencies: Sync + Send {
    // fn get_output(&self) -> &RefCell<dyn Write>;
    fn get_output(&self) -> Arc<BufWriter<Stdout>>;
    fn now(&self) -> SystemTime;
}

/// Struct that holds the dependencies we use when run as the real executable.
pub struct StandardDependencies {
    output: Arc<BufWriter<Stdout>>,
    now: SystemTime,
}

impl StandardDependencies {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // output: Rc::new(RefCell::new(stdout())),
            output: Arc::new(BufWriter::with_capacity(8192, stdout())),
            now: SystemTime::now(),
        }
    }
}

impl Default for StandardDependencies {
    fn default() -> Self {
        Self::new()
    }
}

impl Dependencies for StandardDependencies {
    // fn get_output(&self) -> &RefCell<dyn Write> {
    fn get_output(&self) -> Arc<BufWriter<Stdout>> {
        Arc::clone(&self.output)
    }

    fn now(&self) -> SystemTime {
        self.now
    }
}

unsafe impl Sync for StandardDependencies {}
unsafe impl Send for StandardDependencies {}

static PROCESSED_DIRS: LazyLock<RwLock<HashSet<String>>> = LazyLock::new(|| RwLock::new(HashSet::new()));
// #[global_allocator]
// static A: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// The result of parsing the command-line arguments into useful forms.
struct ParsedInfo {
    matcher: Box<dyn self::matchers::Matcher>,
    paths: Vec<String>,
    config: Config,
}

unsafe impl Sync for ParsedInfo {}
unsafe impl Send for ParsedInfo {}

/// Function to generate a `ParsedInfo` from the strings supplied on the command-line.
fn parse_args(args: &[&str]) -> Result<ParsedInfo, Box<dyn Error>> {
    let mut paths = vec![];
    let mut i = 0;
    let mut config = Config::default();

    while i < args.len() {
        match args[i] {
            "-O0" | "-O1" | "-O2" | "-O3" => {
                // GNU find optimization level flag (ignored)
            }
            "-H" => config.follow = Follow::Roots,
            "-L" => config.follow = Follow::Always,
            "-P" => config.follow = Follow::Never,
            "--" => {
                // End of flags
                i += 1;
                break;
            }
            _ => break,
        }

        i += 1;
    }

    let paths_start = i;
    while i < args.len()
        && (args[i] == "-" || !args[i].starts_with('-'))
        && args[i] != "!"
        && args[i] != "("
    {
        paths.push(args[i].to_string());
        i += 1;
    }
    if i == paths_start {
        paths.push(".".to_string());
    }
    let matcher = matchers::build_top_level_matcher(&args[i..], &mut config)?;
    if let Some(new_paths) = &config.new_paths {
        if paths.len() == 1 && paths[0] == "." {
            paths = new_paths.to_vec();
        } else {
            return Err(From::from(format!(
                "extra operand '{}'\nfile operands cannot be combined with -files0-from",
                paths[0]
            )));
        }
    }
    Ok(ParsedInfo {
        matcher,
        paths,
        config,
    })
}

fn process_dir(
    // dir: &str,
    // dirs: Vec<String>,
    dirs: impl IntoIterator<Item = String>,
    config: &Config,
    deps: &dyn Dependencies,
    matcher: &dyn matchers::Matcher,
    quit: &mut bool,
    dir_ret: &AtomicI32,
    depth: usize,
) -> Vec<WalkEntry> {
    let mut children_dirs = Vec::new();
    let mut ret = 0;
    for dir in dirs {
        // SAFETY: this lock is not held across an await point, so this
        // is all good!
        if matches!(config.follow, Follow::Always) | matches!(config.follow, Follow::Roots) {
            if let Some(_) = PROCESSED_DIRS.read().unwrap().get(&dir) {
                // println!("Dir is {dir}");
                continue;
            }
        }
    
        let mut walkdir = WalkDir::new(&dir)
            .contents_first(config.depth_first)
            .max_depth(depth)
            .min_depth(depth)
            .same_file_system(config.same_file_system)
            .follow_links(config.follow == Follow::Always)
            .follow_root_links(config.follow != Follow::Never);
        if config.sorted_output {
            walkdir = walkdir.sort_by(|a, b| a.file_name().cmp(b.file_name()));
        }

        // Slightly yucky loop handling here :-(. See docs for
        // WalkDirIterator::skip_current_dir for explanation.
        let mut it = walkdir.into_iter();
        // As WalkDir seems not providing a function to check its stack,
        // using current_dir is a workaround to check leaving directory.
        let mut current_dir: Option<PathBuf> = None;
        // let mut matcher_io = matchers::MatcherIO::new(deps);
        while let Some(result) = it.next() {
            match WalkEntry::from_walkdir(result, config.follow) {
                Err(err) => {
                    ret = 1;
                    writeln!(&mut stderr(), "Error: {err}").unwrap();
                }
                Ok(entry) => {
                    let mut matcher_io = matchers::MatcherIO::new(deps);

                    let new_dir = entry.path().parent().map(|x| x.to_path_buf());
                    if new_dir != current_dir {
                        if let Some(dir) = current_dir.take() {
                            matcher.finished_dir(dir.as_path(), &mut matcher_io);
                        }
                        current_dir = new_dir;
                    }
                    
                    // println!("Entry is {:?}", &entry);
                    matcher.matches(&entry, &mut matcher_io);
                    match matcher_io.exit_code() {
                        0 => {}
                        code => ret = code,
                    }
                    if matcher_io.should_quit() {
                        *quit = true;
                        break;
                    }
                    if matcher_io.should_skip_current_dir() {
                        it.skip_current_dir();
                    }

                    if entry.file_type().is_dir() {
                        children_dirs.push(entry);
                    }
                }
            }
        }

        let mut matcher_io = matchers::MatcherIO::new(deps);
        if let Some(dir) = current_dir.take() {
            matcher.finished_dir(dir.as_path(), &mut matcher_io);
        }
        matcher.finished(&mut matcher_io);
        // This is implemented for exec +.
        match matcher_io.exit_code() {
            0 => {}
            code => ret = code,
        }

        if dir_ret.load(Ordering::Relaxed) != 0 {
            dir_ret.store(ret, Ordering::Relaxed);
        }
        // ret

        // SAFETY: this lock is not held across an await point, so this
        // is all good and we can officially denote this as visited
        if matches!(config.follow, Follow::Always) | matches!(config.follow, Follow::Roots) {
            PROCESSED_DIRS.write().unwrap().insert(dir);
        }
    }
    children_dirs
}

async fn do_find(args: &[&str], deps: Box<dyn Dependencies>) -> Result<i32, Box<dyn Error>> {
    let paths_and_matcher = parse_args(args)?;
    if paths_and_matcher.config.help_requested {
        print_help();
        return Ok(0);
    }
    if paths_and_matcher.config.version_requested {
        print_version();
        return Ok(0);
    }

    let ret = Arc::new(AtomicI32::new(0));
    let mut quit = false;

    let handle = Handle::current();
    let metrics = handle.metrics();
    let num_workers = metrics.num_workers();
    // let srb = ShardedRingBuf::<Vec<String>>::new_with_enq_num(num_workers, num_workers, 1);
    let srb = CSShardedRingBuf::<Vec<String>>::new_with_enq_num(num_workers, num_workers, 1);
    // let processed_dirs = Arc::new(RwLock::new(HashSet::new()));

    let config = Arc::new(paths_and_matcher.config);
    let matcher = Arc::new(paths_and_matcher.matcher);
    let deps = Arc::new(deps);

    for path in paths_and_matcher.paths {
        // println!("something is happening");
        let config_clone = config.clone();
        let matcher_clone = matcher.clone();
        let deps_clone = deps.clone();
        // let ret_clone = ret.clone();
        let mut depth = 0;
        if depth == config.min_depth {
            process_dir(vec![path.clone()], &config_clone, &**deps_clone, &*matcher_clone, &mut quit, &ret, 0);
        }
        // let dir_ret = process_dir(
        //     &path,
        //     &paths_and_matcher.config,
        //     deps,
        //     &*paths_and_matcher.matcher,
        //     &mut quit,
        // );
        // if dir_ret != 0 {
        //     ret = dir_ret;
        // }
        // if quit {
        //     break;
        // }
        let (sender, receiver) = kanal::unbounded();
        let _ = sender.send(path);
        loop {
            // println!("sender: {:?}", sender.len());
            // when the receiver is empty or if we reached our max depth, 
            // there are no more directories we need to check
            if depth > config.max_depth || receiver.is_empty() {
                // println!("hello");
                break;
            }
            
            // to spawn only the exact number of dequeuers needed,
            // we grab either the minimum of receiver or number of
            // cores we have as well as use that information to
            // decide how to distribute the dirs evenly among dequeuers
            let num_deq = max(1, min(receiver.len(), num_workers));
            let dir_per_deq = max(1, receiver.len() / num_deq); 
            let mut deq_tasks = Vec::with_capacity(num_deq);
            let mut enq_task = Vec::with_capacity(1);

            for deq_task_i in 0..num_deq {
                let deq_handle = tokio::spawn({
                    let srb_clone = srb.clone();
                    let sender_clone = sender.clone();
                    let config = config.clone();
                    let matcher = matcher.clone();
                    let deps = deps.clone();
                    let ret_clone = ret.clone();
                    async move {
                        loop {
                            // println!("hello?");
                            match srb_clone.dequeue_in_shard(deq_task_i).await {
                                Some(dirs) => {
                                    // println!("smth");

                                    // for dir in dirs {
                                        // println!("Dir is {dir}");
                                        // SAFETY: this lock is not held across an await point, so this
                                        // is all good!

                                        // if matches!(config.follow, Follow::Always) | matches!(config.follow, Follow::Roots) {
                                        //     if let Some(_) = processed_dirs_clone.read().unwrap().get(&dir) {
                                        //         // println!("Dir is {dir}");
                                        //         continue;
                                        //     }
                                        // }
                                        // let dirs = srb_clone.dequeue_item(shard_guard);
                                        // println!("{:?}", dirs);
                                        // what do I want process_dir to do?
                                        // take a dir path, then match itself and its descendants,
                                        // collect any directories into a Vec, and send the directory
                                        // descendants into the kanal channel
                                        let children_dirs: Vec<WalkEntry>;
                                        if depth == 0 && config.min_depth != 0 { 
                                            // children_dirs = process_dir(&dir, &config, &**deps, &*matcher, &mut quit, &ret_clone, config.min_depth);
                                            children_dirs = process_dir(dirs, &config, &**deps, &*matcher, &mut quit, &ret_clone, config.min_depth);
                                        } else {
                                            // children_dirs = process_dir(&dir, &config, &**deps, &*matcher, &mut quit, &ret_clone, 1);
                                            children_dirs = process_dir(dirs, &config, &**deps, &*matcher, &mut quit, &ret_clone, 1);
                                        }

                                        // add in any child directories to the sender
                                        for child in children_dirs {
                                            let _ = sender_clone.send(child.into_path().to_string_lossy().into_owned());
                                        }
                                    // }
                                } 
                                None => {
                                    break
                                },
                            }
                        }
                    }
                    }
                );
                deq_tasks.push(deq_handle);
            }

            {
                let enq_handle = tokio::spawn({
                    let srb_clone = srb.clone();
                    let receiver_clone = receiver.clone();
                    async move {
                        let mut counter = 0;
                        let mut shard_ctr = 0; 
                        let mut dirs = Vec::with_capacity(dir_per_deq);
                        let len = receiver_clone.len();
                        let mut recv_count = 0;

                        while recv_count < len {
                            if let Ok(dir) = receiver_clone.recv() {
                                // println!("{dir}");
                                if counter != 0 && counter % dir_per_deq == 0 {
                                    let shard_guard = srb_clone.enqueue_guard_in_shard(shard_ctr % num_deq).await.unwrap();
                                    srb_clone.enqueue(dirs, shard_guard);
                                    dirs = Vec::with_capacity(dir_per_deq);
                                    dirs.push(dir);
                                    counter += 1;
                                    shard_ctr += 1;
                                } else {
                                    dirs.push(dir);
                                    counter += 1;
                                }
                                recv_count += 1;
                            }
                        }
                        if !dirs.is_empty() {
                            let shard_guard = srb_clone.enqueue_guard_in_shard(shard_ctr % num_deq).await.unwrap();
                            srb_clone.enqueue(dirs, shard_guard);
                        }
                    }        
                });
                enq_task.push(enq_handle);
            }

            for enq in enq_task {
                enq.await.unwrap();
            }

            srb.poison();

            // necessary for poison
            let notifier_task = tokio::spawn({
                let srb_clone = srb.clone();
                async move {
                    loop {
                        for i in 0..num_deq {
                            srb_clone.notify_dequeuer_in_shard(i % srb_clone.get_num_of_shards());
                        }
                        yield_now().await;
                    }
                }
            });

            for deq in deq_tasks {
                deq.await.unwrap();
            }

            notifier_task.abort();
            if depth == 0 && config.min_depth != 0 {
                depth = config.min_depth + 1;
            } else {
                depth += 1;
            }
            srb.clear_poison();
            if quit {
                break;
            }
        }
        if quit {
            break;
        }
    }

    // let _ = deps.get_output().flush();

    Ok(ret.load(Ordering::Relaxed))
}

fn print_help() {
    println!(
        r"Usage: find [path...] [expression]

If no path is supplied then the current working directory is used by default.

Early alpha implementation. Currently the only expressions supported are
 -print
 -print0
 -printf
 -name case-sensitive_filename_pattern
 -lname case-sensitive_filename_pattern
 -iname case-insensitive_filename_pattern
 -ilname case-insensitive_filename_pattern
 -regextype type
 -files0-from
 -regex pattern
 -iregex pattern
 -type type_char
    currently type_char can only be f (for file) or d (for directory)
 -size [+-]N[bcwkMG]
 -delete
 -prune
 -not
 -a
 -o[r]
 ,
 ()
 -true
 -false
 -maxdepth N
 -mindepth N
 -d[epth]
 -xdev
 -ctime [+-]N
 -atime [+-]N
 -mtime [+-]N
 -perm [-/]{{octal|u=rwx,go=w}}
 -newer path_to_file
 -exec[dir] executable [args] [{{}}] [more args] ;
 -sorted
    a non-standard extension that sorts directory contents by name before
    processing them. Less efficient, but allows for deterministic output.
"
    );
}

fn print_version() {
    println!("find (Rust) {}", env!("CARGO_PKG_VERSION"));
}

/// Does all the work for find.
///
/// All main has to do is pass in the command-line args and exit the process
/// with the exit code. Note that the first string in args is expected to be
/// the name of the executable.
pub async fn find_main(args: &[&str], deps: Box<dyn Dependencies>) -> i32 {
    match do_find(&args[1..], deps).await {
        Ok(ret) => ret,
        Err(e) => {
            writeln!(&mut stderr(), "find: {e}").unwrap();
            1
        }
    }
}

#[cfg(test)]
mod tests {

    use std::fs;
    use std::io::{Cursor, ErrorKind, Read};
    use std::time::Duration;
    use tempfile::Builder;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    #[cfg(windows)]
    use std::os::windows::fs::symlink_file;

    use crate::find::matchers::time::ChangeTime;
    use crate::find::matchers::MatcherIO;

    use super::*;

    #[cfg(windows)]
    /// Windows-only bodge for converting between path separators.
    pub fn fix_up_slashes(path: &str) -> String {
        path.replace("/", "\\")
    }

    #[cfg(not(windows))]
    /// Do nothing equivalent of the above for non-windows systems.
    pub fn fix_up_slashes(path: &str) -> String {
        path.to_string()
    }

    /// A struct that implements Dependencies, but uses faked implementations,
    /// allowing us to check output, set the time returned by clocks etc.
    pub struct FakeDependencies {
        pub output: RefCell<Cursor<Vec<u8>>>,
        now: SystemTime,
    }

    unsafe impl Sync for FakeDependencies {}
    unsafe impl Send for FakeDependencies {}

    impl<'a> FakeDependencies {
        pub fn new() -> Self {
            Self {
                output: RefCell::new(Cursor::new(Vec::<u8>::new())),
                now: SystemTime::now(),
            }
        }

        pub fn set_time(&mut self, new_time: SystemTime) {
            self.now = new_time;
        }

        pub fn new_matcher_io(&'a self) -> MatcherIO<'a> {
            MatcherIO::new(self)
        }

        pub fn get_output_as_string(&self) -> String {
            let mut cursor = self.output.borrow_mut();
            cursor.set_position(0);
            let mut contents = String::new();
            cursor.read_to_string(&mut contents).unwrap();
            contents
        }
    }

    impl Dependencies for FakeDependencies {
        fn get_output(&self) -> Arc<BufWriter<Stdout>> {
            // TODO: need to change this 
            Mutex::new(BufWriter::new(stdout())).lock();
            todo!();
        }

        fn now(&self) -> SystemTime {
            self.now
        }
    }

    fn create_file_link() {
        #[cfg(unix)]
        if let Err(e) = symlink("abbbc", "test_data/links/link-f") {
            assert!(
                e.kind() == ErrorKind::AlreadyExists,
                "Failed to create sym link: {e:?}"
            );
        }
        #[cfg(windows)]
        if let Err(e) = symlink_file("abbbc", "test_data/links/link-f") {
            assert!(
                e.kind() == ErrorKind::AlreadyExists,
                "Failed to create sym link: {:?}",
                e
            );
        }
    }

    #[test]
    fn parse_args_handles_single_dash() {
        // Apparently "-" should be treated as a directory name.
        let parsed_info = super::parse_args(&["-"]).expect("parsing should succeed");
        assert_eq!(parsed_info.paths, ["-"]);
    }

    #[test]
    fn parse_args_bad_flag() {
        //
        let result = super::parse_args(&["-asdadsafsfsadcs"]);
        if let Err(e) = result {
            assert_eq!(e.to_string(), "Unrecognized flag: '-asdadsafsfsadcs'");
        } else {
            panic!("parse_args should have returned an error");
        }
    }

    #[test]
    fn parse_optimize_flag() {
        let parsed_info =
            super::parse_args(&["-O0", ".", "-print"]).expect("parsing should succeed");
        assert_eq!(parsed_info.paths, ["."]);
    }

    #[test]
    fn parse_h_flag() {
        let parsed_info = super::parse_args(&["-H"]).expect("parsing should succeed");
        assert_eq!(parsed_info.config.follow, Follow::Roots);
    }

    #[test]
    fn parse_l_flag() {
        let parsed_info = super::parse_args(&["-L"]).expect("parsing should succeed");
        assert_eq!(parsed_info.config.follow, Follow::Always);
    }

    #[test]
    fn parse_p_flag() {
        let parsed_info = super::parse_args(&["-P"]).expect("parsing should succeed");
        assert_eq!(parsed_info.config.follow, Follow::Never);
    }

    #[test]
    fn parse_flag_then_double_dash() {
        super::parse_args(&["-P", "--"]).expect("parsing should succeed");
    }

    #[test]
    fn parse_double_dash_then_flag() {
        super::parse_args(&["--", "-P"])
            .err()
            .expect("parsing should fail");
    }

    // #[tokio::test]
    // async fn find_main_not_depth_first() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &["find", &fix_up_slashes("./test_data/simple"), "-sorted"],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/simple\n\
    //              ./test_data/simple/abbbc\n\
    //              ./test_data/simple/subdir\n\
    //              ./test_data/simple/subdir/ABBBC\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_main_depth_first() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/simple"),
    //             "-sorted",
    //             "-depth",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/simple/abbbc\n\
    //              ./test_data/simple/subdir/ABBBC\n\
    //              ./test_data/simple/subdir\n\
    //              ./test_data/simple\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_maxdepth() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-sorted",
    //             "-maxdepth",
    //             "2",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/depth\n\
    //              ./test_data/depth/1\n\
    //              ./test_data/depth/1/2\n\
    //              ./test_data/depth/1/f1\n\
    //              ./test_data/depth/f0\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_maxdepth_depth_first() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-sorted",
    //             "-maxdepth",
    //             "2",
    //             "-depth",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/depth/1/2\n\
    //              ./test_data/depth/1/f1\n\
    //              ./test_data/depth/1\n\
    //              ./test_data/depth/f0\n\
    //              ./test_data/depth\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_prune() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-sorted",
    //             "-print",
    //             ",",
    //             "-name",
    //             "1",
    //             "-prune",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/depth\n\
    //              ./test_data/depth/1\n\
    //              ./test_data/depth/f0\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_zero_maxdepth() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-maxdepth",
    //             "0",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/depth\n")
    //     );
    // }

    // #[tokio::test]
    // async fn find_zero_maxdepth_depth_first() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-maxdepth",
    //             "0",
    //             "-depth",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/depth\n")
    //     );
    // }

    // #[tokio::test]
    // async fn find_mindepth() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-sorted",
    //             "-mindepth",
    //             "3",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/depth/1/2/3\n\
    //              ./test_data/depth/1/2/3/f3\n\
    //              ./test_data/depth/1/2/f2\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_mindepth_depth_first() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-sorted",
    //             "-mindepth",
    //             "3",
    //             "-depth",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/depth/1/2/3/f3\n\
    //              ./test_data/depth/1/2/3\n\
    //              ./test_data/depth/1/2/f2\n"
    //         )
    //     );
    // }

    // #[tokio::test]
    // async fn find_newer() {
    //     // create a temp directory and file that are newer than the static
    //     // files in the source tree.
    //     let new_dir = Builder::new().prefix("find_newer").tempdir().unwrap();

    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             &new_dir.path().to_string_lossy(),
    //             "-newer",
    //             &fix_up_slashes("./test_data/simple/abbbc"),
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         new_dir.path().to_string_lossy().to_string() + "\n"
    //     );

    //     // now do it the other way around, and nothing should be output
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/simple/abbbc"),
    //             "-newer",
    //             &new_dir.path().to_string_lossy(),
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);
    //     assert_eq!(deps.get_output_as_string(), "");
    // }

    // #[tokio::test]
    // async fn find_mtime() {
    //     let meta = fs::metadata("./test_data/simple/subdir/ABBBC").unwrap();

    //     // metadata can return errors like StringError("creation time is not available on this platform currently")
    //     // so skip tests that won't pass due to shortcomings in std::fs.
    //     if let Ok(file_time) = meta.modified() {
    //         file_time_helper(file_time, "-mtime");
    //     }
    // }

    // #[tokio::test]
    // async fn find_ctime() {
    //     let meta = fs::metadata("./test_data/simple/subdir/ABBBC").unwrap();

    //     // metadata can return errors like StringError("creation time is not available on this platform currently")
    //     // so skip tests that won't pass due to shortcomings in std::fs.
    //     if let Ok(file_time) = meta.changed() {
    //         file_time_helper(file_time, "-ctime").await;
    //     }
    // }

    // #[tokio::test]
    // async fn find_atime() {
    //     let meta = fs::metadata("./test_data/simple/subdir/ABBBC").unwrap();

    //     // metadata can return errors like StringError("creation time is not available on this platform currently")
    //     // so skip tests that won't pass due to shortcomings in std::fs.
    //     if let Ok(file_time) = meta.accessed() {
    //         file_time_helper(file_time, "-atime");
    //     }
    // }

    // /// Helper function for the `find_ctime/find_atime/find_mtime` tests.
    // async fn file_time_helper(file_time: SystemTime, arg: &str) {
    //     // check file time matches a file that's old enough
    //     {
    //         let mut deps = Box::new(FakeDependencies::new());
    //         deps.set_time(file_time);

    //         let rc = find_main(
    //             &[
    //                 "find",
    //                 &fix_up_slashes("./test_data/simple/subdir"),
    //                 "-type",
    //                 "f",
    //                 arg,
    //                 "0",
    //             ],
    //             deps,
    //         );

    //         assert_eq!(rc, 0);
    //         assert_eq!(
    //             deps.get_output_as_string(),
    //             fix_up_slashes("./test_data/simple/subdir/ABBBC\n")
    //         );
    //     }

    //     // now Check file time doesn't match a file that's too new
    //     {
    //         let mut deps = Box::new(FakeDependencies::new());
    //         deps.set_time(file_time - Duration::from_secs(1));

    //         let rc = find_main(
    //             &["find", "./test_data/simple/subdir", "-type", "f", arg, "0"],
    //             deps,
    //         );

    //         assert_eq!(rc, 0);
    //         assert_eq!(deps.get_output_as_string(), "");
    //     }
    // }

    // // Because the time when files exist locally is different
    // // from the time when Github Actions pulls them,
    // // it is difficult to write tests that limit a certain time period.
    // //
    // // For example, a Github Action may pull files from a new git commit within a few minutes,
    // // causing the file time to be refreshed to the pull time.
    // // and The files on the local branch may be several days old.
    // //
    // // So this test may not be too accurate and can only ensure that
    // // the function can be correctly identified.
    // #[tokio::test]
    // async fn find_amin_cmin_mmin() {
    //     let args = ["-amin", "-cmin", "-mmin"];
    //     let times = ["-60", "-120", "-240", "+60", "+120", "+240"];

    //     for arg in args {
    //         for time in times {
    //             let deps = Box::new(FakeDependencies::new());
    //             let rc = find_main(&["find", "./test_data/simple/subdir", arg, time], deps);

    //             assert_eq!(rc, 0);
    //         }
    //     }
    // }

    // #[tokio::test]
    // async fn find_size() {
    //     let deps = Box::new(FakeDependencies::new());
    //     // only look at files because the "size" of a directory is a system (and filesystem)
    //     // dependent thing and we want these tests to be universal.
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/size"),
    //             "-type",
    //             "f",
    //             "-size",
    //             "1b",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/size/512bytes\n")
    //     );

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &["find", "./test_data/size", "-type", "f", "-size", "+1b"],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(deps.get_output_as_string(), "");
    // }

    // #[tokio::test]
    // async fn find_name_links() {
    //     create_file_link();

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/links"),
    //             "-name",
    //             "abbbc",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/links/abbbc\n")
    //     );
    // }

    // #[tokio::test]
    // async fn find_lname_links() {
    //     create_file_link();

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/links"),
    //             "-lname",
    //             "abbbc",
    //             "-sorted",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/links/link-f\n")
    //     );
    // }

    // #[tokio::test]
    // async fn find_ilname_links() {
    //     create_file_link();

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/links"),
    //             "-ilname",
    //             "abBbc",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/links/link-f\n")
    //     );
    // }

    // #[tokio::test]
    // async fn find_print_then_quit() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/simple"),
    //             &fix_up_slashes("./test_data/simple"),
    //             "-print",
    //             "-quit",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/simple\n"),
    //     );
    // }

    // #[tokio::test]
    // async fn test_find_newer_xy_all_args() {
    //     // 1. The t parameter is not allowed at the X position.
    //     // 2. Current Linux filesystem do not support Birthed Time queries,
    //     //    so the B parameter will be excluded in linux.
    //     #[cfg(target_os = "linux")]
    //     let x_options = ["a", "c", "m"];
    //     #[cfg(not(target_os = "linux"))]
    //     let x_options = ["a", "B", "c", "m"];
    //     #[cfg(target_os = "linux")]
    //     let y_options = ["a", "c", "m"];
    //     #[cfg(not(target_os = "linux"))]
    //     let y_options = ["a", "B", "c", "m"];

    //     for &x in &x_options {
    //         for &y in &y_options {
    //             let arg = &format!("-newer{x}{y}").to_string();
    //             let deps = Box::new(FakeDependencies::new());
    //             let rc = find_main(
    //                 &[
    //                     "find",
    //                     "./test_data/simple/subdir",
    //                     arg,
    //                     "./test_data/simple/subdir/ABBBC",
    //                 ],
    //                 deps,
    //             );

    //             assert_eq!(rc, 0);

    //             let arg = &format!("-follow -newer{x}{y}").to_string();
    //             let deps = Box::new(FakeDependencies::new());
    //             let rc = find_main(
    //                 &[
    //                     "find",
    //                     "./test_data/simple/subdir",
    //                     arg,
    //                     "./test_data/simple/subdir/ABBBC",
    //                 ],
    //                 deps,
    //             );

    //             assert_eq!(rc, 0);
    //         }
    //     }
    // }

    // #[tokio::test]
    // #[cfg(target_os = "linux")]
    // async fn test_find_newer_xy_have_not_birthed_time_filesystem() {
    //     let y_options = ["a", "c", "m"];
    //     for &y in &y_options {
    //         let arg = &format!("-newerB{y}").to_string();
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(
    //             &[
    //                 "find",
    //                 "./test_data/simple/subdir",
    //                 arg,
    //                 "./test_data/simple/subdir/ABBBC",
    //             ],
    //             deps,
    //         );

    //         assert_eq!(rc, 1);
    //     }
    // }

    // #[cfg(unix)]
    // #[tokio::test]
    // async fn test_find_newer_xy_before_changed_time() {
    //     // normal - before the changed time
    //     #[cfg(target_os = "linux")]
    //     let args = ["-newerat", "-newerct", "-newermt"];
    //     #[cfg(not(target_os = "linux"))]
    //     let args = ["-newerat", "-newerBt", "-newerct", "-newermt"];
    //     let times = ["jan 01, 2000", "jan 01, 2000 00:00:00"];

    //     for arg in args {
    //         for time in times {
    //             let deps = Box::new(FakeDependencies::new());
    //             let rc = find_main(&["find", "./test_data/simple/subdir", arg, time], deps);

    //             assert_eq!(rc, 0);
    //             assert!(deps
    //                 .get_output_as_string()
    //                 .contains("./test_data/simple/subdir"));
    //             assert!(deps.get_output_as_string().contains("ABBBC"));
    //         }
    //     }
    // }

    // #[tokio::test]
    // async fn test_find_newer_xy_after_changed_time() {
    //     // normal - after the changed time
    //     #[cfg(target_os = "linux")]
    //     let args = ["-newerat", "-newerct", "-newermt"];
    //     #[cfg(not(target_os = "linux"))]
    //     let args = ["-newerat", "-newerBt", "-newerct", "-newermt"];
    //     let times = ["jan 01, 2037", "jan 01, 2037 00:00:00"];

    //     for arg in args {
    //         for time in times {
    //             let deps = Box::new(FakeDependencies::new());
    //             let rc = find_main(&["find", "./test_data/simple/subdir", arg, time], deps);

    //             assert_eq!(rc, 0);
    //             assert_eq!(deps.get_output_as_string(), "");
    //         }
    //     }
    // }

    // #[tokio::test]
    // async fn test_find_newer_xy_empty_time_parameter() {
    //     // When an empty time parameter is passed in,
    //     // the program will use 00:00 of the current day as the default time.
    //     // Therefore, the files checkout of the git repository while
    //     // this test was running are likely to be newer than the default time.
    //     #[cfg(target_os = "linux")]
    //     let args = ["-newerat", "-newerct", "-newermt"];
    //     #[cfg(not(target_os = "linux"))]
    //     let args = ["-newerat", "-newerBt", "-newerct", "-newermt"];
    //     let time = "";

    //     for &arg in &args {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(&["find", "./test_data/simple/subdir", arg, time], deps);

    //         assert_eq!(rc, 0);
    //         // Output comparison has been temporarily removed to account for the possibility that
    //         // migration out of the repository started before 00:00 and testing was completed after 00:00.
    //     }
    // }

    // #[tokio::test]
    // async fn test_find_newer_xy_error_time() {
    //     // Catch a parsing error.
    //     #[cfg(target_os = "linux")]
    //     let args = ["-newerat", "-newerct", "-newermt"];
    //     #[cfg(not(target_os = "linux"))]
    //     let args = ["-newerat", "-newerBt", "-newerct", "-newermt"];
    //     let time = "2037, jan 01";

    //     for &arg in &args {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(&["find", "./test_data/simple/subdir", arg, time], deps);

    //         assert_eq!(rc, 1);
    //     }
    // }

    // #[tokio::test]
    // #[cfg(target_os = "linux")]
    // async fn test_no_permission_file_error() {
    //     use std::{path::Path, process::Command};

    //     let path = Path::new("./test_data/no_permission");
    //     let _result = fs::create_dir(path);
    //     // Generate files without permissions.
    //     // std::fs cannot change file permissions to 000 in normal user state,
    //     // so use chmod via Command to change permissions.
    //     let _output = Command::new("chmod")
    //         .arg("-rwx")
    //         .arg("./test_data/no_permission")
    //         .output()
    //         .expect("cannot set file permission");

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/no_permission"], deps);

    //     assert_eq!(rc, 1);

    //     if path.exists() {
    //         let _result = fs::create_dir(path);
    //         // Remove the unreadable and writable status of the file to avoid affecting other tests.
    //         let _output = Command::new("chmod")
    //             .arg("+rwx")
    //             .arg("./test_data/no_permission")
    //             .output()
    //             .expect("cannot set file permission");
    //     }
    // }

    // #[tokio::test]
    // #[cfg(target_os = "linux")]
    // async fn test_user_predicate() {
    //     use std::{os::unix::fs::MetadataExt, path::Path};

    //     use nix::unistd::{Uid, User};

    //     let path = Path::new("./test_data/simple/subdir");
    //     let uid = path.metadata().unwrap().uid();
    //     let user = User::from_uid(Uid::from_raw(uid)).unwrap().unwrap().name;

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &["find", "./test_data/simple/subdir", "-user", &user],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         "./test_data/simple/subdir\n./test_data/simple/subdir/ABBBC\n"
    //     );

    //     // test uid
    //     for arg in ["-uid", "-user"] {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(
    //             &["find", "./test_data/simple/subdir", arg, &uid.to_string()],
    //             deps,
    //         );
    //         assert_eq!(rc, 0);
    //     }

    //     // test -uid +N, -uid -N
    //     if uid > 0 {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(
    //             &[
    //                 "find",
    //                 "./test_data/simple/subdir",
    //                 "-uid",
    //                 &format!("+{}", uid - 1),
    //                 "-uid",
    //                 &format!("-{}", uid + 1),
    //             ],
    //             deps,
    //         );
    //         assert_eq!(rc, 0);
    //         assert_eq!(
    //             deps.get_output_as_string(),
    //             "./test_data/simple/subdir\n./test_data/simple/subdir/ABBBC\n"
    //         );
    //     }

    //     // test empty uid
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-uid", ""], deps);
    //     assert_eq!(rc, 1);

    //     // test not a number
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-uid", "a"], deps);
    //     assert_eq!(rc, 1);

    //     // test empty user name
    //     ["-user", "-nouser"].iter().for_each(|&arg| {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(&["find", "./test_data/simple/subdir", arg, ""], deps);

    //         assert_eq!(rc, 1);

    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(&["find", "./test_data/simple/subdir", arg, " "], deps);

    //         assert_eq!(rc, 1);
    //     });
    // }

    // #[tokio::test]
    // #[cfg(target_os = "linux")]
    // async fn test_nouser_predicate() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-nouser"], deps);

    //     assert_eq!(rc, 0);
    //     assert_eq!(deps.get_output_as_string(), "");
    // }

    // #[tokio::test]
    // #[cfg(target_os = "linux")]
    // async fn test_group_predicate() {
    //     use std::{os::unix::fs::MetadataExt, path::Path};

    //     use nix::unistd::{Gid, Group};

    //     let path = Path::new("./test_data/simple/subdir");
    //     let gid = path.metadata().unwrap().gid();
    //     let group = Group::from_gid(Gid::from_raw(gid)).unwrap().unwrap().name;

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &["find", "./test_data/simple/subdir", "-group", &group],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         "./test_data/simple/subdir\n./test_data/simple/subdir/ABBBC\n"
    //     );

    //     // test gid
    //     for arg in ["-gid", "-group"] {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(
    //             &["find", "./test_data/simple/subdir", arg, &gid.to_string()],
    //             deps,
    //         );
    //         assert_eq!(rc, 0);
    //     }

    //     // test -gid +N, -gid -N
    //     if gid > 0 {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(
    //             &[
    //                 "find",
    //                 "./test_data/simple/subdir",
    //                 "-gid",
    //                 &format!("+{}", gid - 1),
    //                 "-gid",
    //                 &format!("-{}", gid + 1),
    //             ],
    //             deps,
    //         );
    //         assert_eq!(rc, 0);
    //         assert_eq!(
    //             deps.get_output_as_string(),
    //             "./test_data/simple/subdir\n./test_data/simple/subdir/ABBBC\n"
    //         );
    //     }

    //     // test empty gid
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-gid", ""], deps);
    //     assert_eq!(rc, 1);

    //     // test not a number
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-gid", "a"], deps);
    //     assert_eq!(rc, 1);

    //     // test empty user name and group name
    //     ["-group", "-nogroup"].iter().for_each(|&arg| {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(&["find", "./test_data/simple/subdir", arg, ""], deps);

    //         assert_eq!(rc, 1);

    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(&["find", "./test_data/simple/subdir", arg, " "], deps);

    //         assert_eq!(rc, 1);
    //     });
    // }

    // #[tokio::test]
    // #[cfg(target_os = "linux")]
    // async fn test_nogroup_predicate() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-nogroup"], deps);

    //     assert_eq!(rc, 0);
    //     assert_eq!(deps.get_output_as_string(), "");
    // }

    // #[tokio::test]
    // #[cfg(unix)]
    // async fn test_fs_matcher() {
    //     use crate::find::tests::FakeDependencies;
    //     use matchers::fs::get_file_system_type;
    //     use std::cell::RefCell;
    //     use std::path::Path;

    //     let path = Path::new("./test_data/simple/subdir");
    //     let empty_cache = RefCell::new(None);
    //     let target_fs_type = get_file_system_type(path, &empty_cache).unwrap();

    //     // should match fs type
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             "./test_data/simple/subdir",
    //             "-fstype",
    //             &target_fs_type,
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    // }

    // #[tokio::test]
    // #[cfg(unix)]
    // async fn test_noleaf() {
    //     use crate::find::tests::FakeDependencies;

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-noleaf"], deps);

    //     assert_eq!(rc, 0);
    // }

    // #[tokio::test]
    // async fn find_maxdepth_and() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             &fix_up_slashes("./test_data/depth"),
    //             "-maxdepth",
    //             "0",
    //             "-a",
    //             "-print",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/depth\n")
    //     );
    // }

    // #[tokio::test]
    // #[cfg(unix)]
    // async fn test_daystart() {
    //     use crate::find::tests::FakeDependencies;

    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             "./test_data/simple/subdir",
    //             "-daystart",
    //             "-mtime",
    //             "0",
    //         ],
    //         deps,
    //     ).await;

    //     assert_eq!(rc, 0);

    //     // twice -daystart should be matched
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             "./test_data/simple/subdir",
    //             "-daystart",
    //             "-daystart",
    //             "-mtime",
    //             "1",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    // }

    // #[tokio::test]
    // async fn find_fprinter() {
    //     let printer = ["fprint", "fprint0"];

    //     for p in &printer {
    //         let deps = Box::new(FakeDependencies::new());
    //         let rc = find_main(
    //             &[
    //                 "find",
    //                 "./test_data/simple",
    //                 format!("-{p}").as_str(),
    //                 format!("test_data/find_{p}").as_str(),
    //             ],
    //             deps,
    //         );
    //         assert_eq!(rc, 0);

    //         let _ = fs::remove_file(format!("test_data/find_{p}"));
    //     }
    // }

    // #[tokio::test]
    // async fn test_follow() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple", "-follow"], deps);
    //     assert_eq!(rc, 0);
    // }

    // #[cfg(unix)]
    // #[tokio::test]
    // async fn test_h_flag() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &["find", "-H", &fix_up_slashes("./test_data/links/link-d")],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/links/link-d\n\
    //              ./test_data/links/link-d/test\n"
    //         )
    //     );
    // }

    // #[cfg(unix)]
    // #[tokio::test]
    // async fn test_l_flag() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &[
    //             "find",
    //             "-L",
    //             &fix_up_slashes("./test_data/links"),
    //             "-sorted",
    //         ],
    //         deps,
    //     );

    //     assert_eq!(rc, 1);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes(
    //             "./test_data/links\n\
    //              ./test_data/links/abbbc\n\
    //              ./test_data/links/link-d\n\
    //              ./test_data/links/link-d/test\n\
    //              ./test_data/links/link-f\n\
    //              ./test_data/links/link-missing\n\
    //              ./test_data/links/link-notdir\n\
    //              ./test_data/links/subdir\n\
    //              ./test_data/links/subdir/test\n"
    //         )
    //     );
    // }

    // #[cfg(unix)]
    // #[tokio::test]
    // async fn test_p_flag() {
    //     let deps = Box::new(FakeDependencies::new());

    //     let rc = find_main(
    //         &["find", "-P", &fix_up_slashes("./test_data/links/link-d")],
    //         deps,
    //     );

    //     assert_eq!(rc, 0);
    //     assert_eq!(
    //         deps.get_output_as_string(),
    //         fix_up_slashes("./test_data/links/link-d\n")
    //     );
    // }

    // #[tokio::test]
    // async fn find_fprintf() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(
    //         &[
    //             "find",
    //             "./test_data/simple",
    //             "-fprintf",
    //             "test_data/find_fprintf",
    //             "%h %H %p %P",
    //         ],
    //         deps,
    //     );
    //     assert_eq!(rc, 0);

    //     let _ = fs::remove_file("test_data/find_fprintf");
    // }

    // #[tokio::test]
    // #[cfg(unix)]
    // async fn test_ls() {
    //     let deps = Box::new(FakeDependencies::new());
    //     let rc = find_main(&["find", "./test_data/simple/subdir", "-ls"], deps);

    //     assert_eq!(rc, 0);
    // }
}
