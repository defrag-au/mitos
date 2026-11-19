//! The smallest `wasm32-wasip2` program this toolchain will build.
//!
//! Its job is to make the toolchain's `std` declare its `wasi:*` imports, so
//! the platform's linker can be asked to satisfy them; none of it is meant to
//! run. Each statement keeps one of those imports alive under optimisation —
//! `environment` and `stdout` from `args`/`println!`, `exit` from
//! `process::exit`.

fn main() {
    let argv = std::env::args().count();
    println!("probe {argv}");
    std::process::exit(0);
}
