use anyhow::Result;
use clap::{
    builder::{
        styling::{AnsiColor, Effects},
        Styles,
    },
    ArgAction, Parser, Subcommand,
};
use simplelog::*;
mod fmt;

fn styles() -> Styles {
    Styles::styled()
        .header(AnsiColor::Yellow.on_default().effects(Effects::BOLD)) // "Options:" etc.
        .usage(AnsiColor::Green.on_default().effects(Effects::BOLD))
        .literal(AnsiColor::Cyan.on_default()) // flags & subcommands
        .placeholder(AnsiColor::Blue.on_default()) // <VALUE> placeholders
        .valid(AnsiColor::Green.on_default()) // valid values in errors
        .invalid(AnsiColor::Red.on_default()) // invalid values in errors
        .error(AnsiColor::Red.on_default().effects(Effects::BOLD))
}

/// CSV utility tools
#[derive(Debug, Parser)]
#[command(name = "csv")]
#[command(about = "A collection of CSV tools", long_about = None, version)]
#[command(propagate_version = true)]
#[command(color= clap::ColorChoice::Auto, styles=styles())]
struct Cli {
    /// Turn debugging information on
    #[arg(long, action=ArgAction::SetTrue)]
    #[clap(global = true, long)]
    debug: bool,

    /// Choose the operation you want to perform. The default is "fmt"
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    // Format the file or STDIN by justifying all the columns.
    #[command(name = "fmt")]
    Fmt(fmt::cli::FmtArgs),
}

fn main() -> Result<()> {
    let args = Cli::parse();

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
        Commands::Fmt(fmt_args) => fmt::run(fmt_args)?,
        _ => {}
    }

    Ok(())
}
