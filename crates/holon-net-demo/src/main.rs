fn main() {
    let args: Vec<String> = std::env::args().collect();
    let opts = holon_net_demo::roles::Opts::parse(&args);
    std::process::exit(holon_net_demo::roles::run(&opts));
}
