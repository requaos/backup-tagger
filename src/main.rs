use chrono::{Duration, Utc};
use cron_parser::parse;
use chrono::DateTime;
use std::env;

fn main() {    
    let args: Vec<String> = env::args().collect();
    println!("args: {}", args.join("|"));

    let every_n_hours = "4";
    let minutes_offset = "30";
    let standard = format!("{} 0/{} * * *", minutes_offset, every_n_hours);
    let nightly = format!("{} {} * * *", minutes_offset, every_n_hours);
    let weekly = format!("{} {} * * 6", minutes_offset, every_n_hours);
    let monthly = format!("{} {} 1 * *", minutes_offset, every_n_hours);
    let checks: Vec<String> = vec![standard, nightly, weekly, monthly];
    let now = Utc::now();
    // Need to subtract a few seconds to catch the current trigger.
    let now_comparison_value = now.checked_sub_signed(Duration::seconds(5)).unwrap();
    // For debugging.
    let custom = DateTime::parse_from_rfc3339("2025-07-01T04:30:00+00:00").unwrap();
    println!("Custom time input or simulation: {}", custom.to_rfc3339());

    for check in checks {
         if let Ok(next) = parse(check.as_str(), &now_comparison_value) {
              println!("- when: {} match: {}", next.to_rfc3339(), next.eq(&now));
         }
         if let Ok(next) = parse(check.as_str(), &custom.checked_sub_signed(Duration::seconds(1)).unwrap()) {
              println!("  when: {} match: {}", next.to_rfc3339(), next.eq(&custom));
         }
    }
}