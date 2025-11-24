use std::env;
use std::path::PathBuf;

fn main() {

    println!("cargo:rerun-if-changed=wrapper.h");

    // Find dqlite header
    pkg_config::Config::new()
        .probe("dqlite")
        .expect("dqlite dev library not found; install libdqlite-dev");

    let bindings = bindgen::Builder::default()
        .header("/usr/include/dqlite.h")
        .allowlist_function("dqlite_.*")
        .allowlist_type("dqlite.*")
        .allowlist_var("DQLITE_.*")
        .rust_target("1.81.0".parse().unwrap()) // rust-bindgen issue #3052 solution
        .layout_tests(false) // solves unstable library feature 'offset_of'
        .generate()
        .expect("bindgen failed");

    bindings
        .write_to_file("src/bindings.rs")
        .expect("couldn't write bindings");

    println!("cargo:rustc-link-lib=dqlite");
    println!("cargo:rustc-link-lib=uv");
    println!("cargo:rustc-link-lib=sqlite3");
    println!("cargo:rustc-link-lib=lz4");
}