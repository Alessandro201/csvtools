use anyhow::Result;
use clap::{Parser, Subcommand};
mod fmt;

/// CSV utility tools
#[derive(Debug, Parser)]
#[command(name = "csv")]
#[command(about = "A collection of CSV tools", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(name = "fmt")]
    Fmt(fmt::FmtArgs),
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Fmt(fmt_args) => fmt::process(fmt_args)?,
    };

    Ok(())
}
