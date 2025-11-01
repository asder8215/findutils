// Copyright 2017 Google Inc.
//
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

#[tokio::main]
async fn main() {
    // Ignores the SIGPIPE signal.
    // This is to solve the problem that when find is used with a pipe character,
    // the downstream software of the standard output stream closes the pipe and triggers a panic.
    uucore::panic::mute_sigpipe_panic();

    let args = std::env::args().collect::<Vec<String>>();
    let strs: Vec<&str> = args.iter().map(std::convert::AsRef::as_ref).collect();
    let deps = Box::new(findutils::find::StandardDependencies::new());
    let status_code = findutils::find::find_main(&strs, deps).await;
    std::process::exit(status_code);
}
