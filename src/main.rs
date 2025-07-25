use chrono::{Duration, Utc};
use clap::{command, Arg, Parser};
use color_eyre::eyre::ContextCompat;
use color_eyre::{eyre::Report, eyre::WrapErr, Section};
use cron_parser::parse;
use std::env;
use tracing::{info, instrument};

/// Backup TiKV/SurrealDB S3 Tags
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Matching every n hours
    #[arg(short, long, default_value_t = String::from("4"))]
    every_n_hours: String,

    /// Minutes forward from the top of the hour to offset match by
    #[arg(short, long, default_value_t = String::from("30"))]
    minutes_offset: String,

    /// Matching window for clock skew and/or job trigger delay
    #[arg(short, long, default_value_t = String::from("20"))]
    lag_window_in_minutes: String,
}

#[instrument]
fn main() -> Result<(), Report> {
    install_tracing();
    color_eyre::install()?;

    info!("Processing CLI flags");
    let args = Args::parse();
    let lag = args
        .lag_window_in_minutes
        .parse()
        .wrap_err("Unable to parse lag-window-in-minutes argument value")?;
    let checks = periods(&args.minutes_offset, &args.every_n_hours);

    info!("Capturing current UTC time and adjusting within lag window");
    let now = Utc::now();
    // Need to subtract a few minutes to catch the current trigger.
    // 1/4 of the lag window feels right.
    let now_comparison_value = now
        .checked_sub_signed(Duration::minutes(lag / 4))
        .wrap_err("Unable to apply jitter to current UTC timestamp")
        .suggestion("Check the system clock")?;

    info!("Processing list of tag checks");
    let mut tags: Vec<String> = Vec::new();
    for check in checks {
        if let Ok(next) = parse(check.0.as_str(), &now_comparison_value) {
            let diff = if check.2 {
                next.checked_sub_signed(Duration::days(1))
                    .wrap_err("Unable to adjust next matching run time for period end")
                    .suggestion("Check the system clock")?
            } else {
                next
            } - now;
            if diff.num_seconds().abs() < lag {
                tags.push(check.1.clone());
            }
            info!(target: "match_attempt_results", tag = check.1, when = next.to_rfc3339(), matched = diff.num_seconds().abs() < lag);
        }
    }

    let output = format!(
        "{{\"TagSet\":[{{\"Key\":\"standard\",\"Value\":\"1\"}}{}]}}",
        tags.join("")
    );
    info!(output);
    print!("{}", output);
    Ok(())
}

fn periods(minutes_offset: &String, every_n_hours: &String) -> Vec<(String, String, bool)> {
    // always tag as standard, so manual runs get tagged for lifecycle rules
    // let standard = (
    //     format!("{} 0/{} * * *", minutes_offset, every_n_hours),
    //     String::from("{\"Key\":\"standard\",\"Value\":\"1\"}"),
    //     false,
    // );
    let nightly = (
        format!("{} {} * * *", minutes_offset, every_n_hours),
        String::from(",{\"Key\":\"nightly\",\"Value\":\"1\"}"),
        false,
    );
    let weekly = (
        format!("{} {} * * 6", minutes_offset, every_n_hours),
        String::from(",{\"Key\":\"weekly\",\"Value\":\"1\"}"),
        false,
    );
    let monthly = (
        format!("{} {} 1 * *", minutes_offset, every_n_hours),
        String::from(",{\"Key\":\"monthly\",\"Value\":\"1\"}"),
        true,
    );
    let quarterly = (
        format!("{} {} 1 */3 *", minutes_offset, every_n_hours),
        String::from(",{\"Key\":\"quarterly\",\"Value\":\"1\"}"),
        true,
    );
    let yearly = (
        format!("{} {} 1 1 *", minutes_offset, every_n_hours),
        String::from(",{\"Key\":\"yearly\",\"Value\":\"1\"}"),
        true,
    );
    return vec![nightly, weekly, monthly, quarterly, yearly];
}

fn install_tracing() {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{fmt, EnvFilter};

    let fmt_layer = fmt::layer().with_target(false);
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(ErrorLayer::default())
        .init();
}
