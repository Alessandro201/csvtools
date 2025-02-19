use std::env;

use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use simplelog::*;
mod fmt;

/// CSV utility tools
#[derive(Debug, Parser)]
#[command(name = "csv")]
#[command(about = "A collection of CSV tools", long_about = None, version)]
#[command(propagate_version = true)]
struct Cli {
    /// Turn debugging information on
    #[arg(long, action=ArgAction::SetTrue)]
    #[clap(global = true, long)]
    debug: bool,

    /// Choose the operation you want to perform. The default is "fmt"
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    // Format the file or STDIN by justifying all the columns. This is the default option which
    // will be run when no subcommands is given
    #[command(name = "fmt")]
    Fmt(fmt::FmtArgs),
}

fn help_and_exit() -> ! {
    use clap::CommandFactory;
    Cli::command().print_help().expect("Unable to write help");
    std::process::exit(1)
}

fn main() -> Result<()> {
    let mut env_args: Vec<String> = env::args().collect();
    if env_args.len() < 2
        || (env_args.len() == 2 && ["-h", "--help"].contains(&env_args[1].as_str()))
    {
        help_and_exit()
    }
    // No subcommand given, or the subommand is one of the known ones
    if env_args[1].starts_with("-") || !["fmt", "help"].contains(&env_args[1].as_str()) {
        env_args.insert(1, "fmt".to_string());
    }
    let args = Cli::parse_from(env_args);

    if args.debug {
        TermLogger::init(
            LevelFilter::Debug,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )?
    } else {
        TermLogger::init(
            LevelFilter::Warn,
            Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )?
    }

    debug!("{:#?}", &args);
    match args.command {
        Some(Commands::Fmt(fmt_args)) => fmt::process(fmt_args)?,
        None => {}
    }

    Ok(())
}
