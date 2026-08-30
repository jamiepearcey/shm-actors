//! The `holon-demo` role binary (see the library docs for the roles).

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let opts = holon_demo::roles::Opts::parse(&args);
    std::process::exit(holon_demo::roles::run(&opts));
}
