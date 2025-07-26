use chrono::{DateTime, Duration, Utc};
use clap::{command, Arg, Parser};
use color_eyre::eyre::{ContextCompat, Result};
use color_eyre::{eyre::Report, eyre::WrapErr, Section};
use cron_parser::parse;
use serde::{Deserialize, Serialize};
use std::env;
use std::process::{Command, Output, Stdio};
use tracing::{info, instrument};
use valuable::Valuable;

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

#[derive(Serialize, Valuable)]
#[serde(rename_all = "PascalCase")]
struct TagSet {
    tag_set: Vec<Tag>,
}

#[derive(Serialize, Clone, Valuable)]
#[serde(rename_all = "PascalCase")]
struct Tag {
    key: String,
    value: String,
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

    // Add default tag every time
    let mut tags: Vec<Tag> = Vec::new();
    tags.push(Tag {
        key: String::from("standard"),
        value: String::from("1"),
    });

    info!("Processing list of tag checks");
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
            info!(target: "match_attempt_results", tag = check.1.as_value(), when = next.to_rfc3339(), matched = diff.num_seconds().abs() < lag);
        }
    }

    let output = serde_json::to_string(&TagSet { tag_set: tags })?;
    info!(output);
    print!("{}", output);
    Ok(())
}

fn periods(minutes_offset: &String, every_n_hours: &String) -> Vec<(String, Tag, bool)> {
    // always tag as standard, so manual runs get tagged for lifecycle rules
    // let standard = (
    //     format!("{} 0/{} * * *", minutes_offset, every_n_hours),
    //     Tag {
    //         key: String::from("standard"),
    //         value: String::from("1"),
    //     },
    //     false,
    // );
    let nightly = (
        format!("{} {} * * *", minutes_offset, every_n_hours),
        Tag {
            key: String::from("nightly"),
            value: String::from("1"),
        },
        false,
    );
    let weekly = (
        format!("{} {} * * 6", minutes_offset, every_n_hours),
        Tag {
            key: String::from("weekly"),
            value: String::from("1"),
        },
        false,
    );
    let monthly = (
        format!("{} {} 1 * *", minutes_offset, every_n_hours),
        Tag {
            key: String::from("monthly"),
            value: String::from("1"),
        },
        true,
    );
    let quarterly = (
        format!("{} {} 1 */3 *", minutes_offset, every_n_hours),
        Tag {
            key: String::from("quarterly"),
            value: String::from("1"),
        },
        true,
    );
    let yearly = (
        format!("{} {} 1 1 *", minutes_offset, every_n_hours),
        Tag {
            key: String::from("yearly"),
            value: String::from("1"),
        },
        true,
    );
    return vec![nightly, weekly, monthly, quarterly, yearly];
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListObjectResult {
    contents: Vec<Object>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Object {
    key: String,
}

fn tikv_backup_with_defaults(
    time: DateTime<Utc>,
    bin_path: String,
    bucket_name: String,
    tags: String,
    s3_endpoint: Option<(String, String, String)>,
) -> Result<Output, Report> {
    return tikv_backup(
        time,
        bin_path,
        bucket_name,
        String::from("tidb-cluster-pd.tidb-admin:2379"),
        tags,
        s3_endpoint,
    );
}

fn tikv_backup(
    time: DateTime<Utc>,
    bin_path: String,
    bucket_name: String,
    pd_host_and_port: String,
    tags: String,
    s3_endpoint: Option<(String, String, String)>,
) -> Result<Output, Report> {
    let storage_key = format!("tikv/{}", time.format("+%Y-%m-%d.%H-%M"));
    // Existing values:
    // tikv-br backup raw --pd=tidb-cluster-pd.tidb-admin:2379 --send-credentials-to-tikv=false
    let mut aws_command = Command::new(format!("{}/bin/aws", bin_path));
    let endpoint_is_some = s3_endpoint.is_some();
    let mut aws_endpoint: String = String::new();
    let mut aws_id: String = String::new();
    let mut aws_key: String = String::new();
    if let Some(s3_endpoint) = s3_endpoint {
        aws_endpoint = s3_endpoint.0;
        aws_id = s3_endpoint.1;
        aws_key = s3_endpoint.2;
    }
    // We want to pass in the TiKV PD address and port
    let tikv_br_command_result = Command::new(format!("{}/bin/tikv-br", bin_path))
        .arg("backup")
        .arg("raw")
        .arg(format!("--pd={}", pd_host_and_port))
        .arg(format!("--send-credentials-to-tikv={}", endpoint_is_some))
        .arg(format!("--storage=s3://${}/{}", bucket_name, storage_key))
        .output()
        .wrap_err("failed to execute process");

    let s3_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", aws_id)
            .env("AWS_SECRET_ACCESS_KEY", aws_key)
            .arg("s3api")
            .arg("list-objects")
            .arg(format!("--endpoint-url {}", aws_endpoint))
            .arg(format!("--bucket {}", bucket_name))
            .arg(format!("--prefix {}", storage_key))
            .arg("--output json")
            .output()
            .wrap_err("failed to execute process")?
    } else {
        aws_command
            .arg("s3api")
            .arg("list-objects")
            .arg(format!("--bucket {}", bucket_name))
            .arg(format!("--prefix {}", storage_key))
            .arg("--output json")
            .output()
            .wrap_err("failed to execute process")?
    };
    // TODO: list all the files that were pushed up by the distributed backup command.
    // LIST_RESP=`${nixpkgs.awscli}/bin/aws s3api list-objects --bucket ${backupBucket} --prefix $KEY --output json`

    let list_object_result = serde_json::from_str::<ListObjectResult>(
        String::from_utf8(s3_command_output.stdout)?.as_str(),
    )?;
    let object_keys = list_object_result
        .contents
        .iter()
        .map(|o| o.key.as_str())
        .collect::<Vec<_>>();
    // KEYS=`${nixpkgs.jq}/bin/jq '.Contents[] | .Key' <<< "$LIST_RESP"`
    // ${echo} $KEYS | ${nixpkgs.uutils-coreutils-noprefix}/bin/tr " " "\n"

    for key in object_keys {
        let _s3_command_output = aws_command
            .arg("s3api")
            .arg("put-object-tagging")
            .arg(format!("--bucket {}", bucket_name))
            .arg(format!("--tagging {}", tags))
            .arg(format!("--key {}", key))
            .output()
            .wrap_err("failed to execute process");
    }
    // TODO: Apply tags to all keys returned from list operation.
    // ${nixpkgs.findutils}/bin/xargs -rP 4 -n 1 ${nixpkgs.awscli}/bin/aws s3api put-object-tagging \
    // --bucket ${backupBucket} \
    // --tagging "{\"TagSet\":[{\"Key\":\"thirdofhalfday\",\"Value\":\"1\"}$TAGS]}" \
    // --key <<< "$KEYS"

    return tikv_br_command_result;
}

fn surrealdb_backup(
    time: DateTime<Utc>,
    bin_path: String,
    bucket_name: String,
    namespace: String,
    address: String,
    password: String,
    tags: String,
    s3_endpoint: Option<(String, String, String)>,
) -> Result<Output, Report> {
    let storage_key = format!("surrealdb/{}/{}", namespace, time.format("+%Y-%m-%d.%H-%M"));
    // KEY=surrealdb/$NS/${ds}.zst

    let surrealdb_command_output = Command::new(format!("{}/bin/surreal", bin_path))
        .arg("export")
        .arg(format!("-e http://{}", address))
        .arg("-u root")
        .arg(format!("-p {}", password))
        .arg(format!("--namespace {}", namespace))
        .arg("--database calamu")
        .arg("-")
        .stdout(Stdio::piped())
        .spawn()
        .wrap_err("failed to execute process")?;
    let zstd_command_output = Command::new(format!("{}/bin/zstd", bin_path))
        .stdin(surrealdb_command_output.stdout.unwrap())
        .arg("--force")
        .arg("--stdout")
        .arg("--adapt")
        .arg("--rm")
        .arg("-")
        .stdout(Stdio::piped())
        .spawn()
        .wrap_err("failed to execute process")?;

    let mut aws_command = Command::new(format!("{}/bin/aws", bin_path));
    let endpoint_is_some = s3_endpoint.is_some();
    let mut aws_endpoint: String = String::new();
    let mut aws_id: String = String::new();
    let mut aws_key: String = String::new();
    if let Some(s3_endpoint) = s3_endpoint {
        aws_endpoint = s3_endpoint.0;
        aws_id = s3_endpoint.1;
        aws_key = s3_endpoint.2;
    }
    let s3_cp_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", aws_id.clone())
            .env("AWS_SECRET_ACCESS_KEY", aws_key.clone())
            .stdin(zstd_command_output.stdout.unwrap())
            .arg("s3")
            .arg("cp")
            .arg(format!("--endpoint-url {}", aws_endpoint.clone()))
            .arg("-")
            .arg(format!("s3://{}/{}", bucket_name, storage_key))
            .output()
            .wrap_err("failed to execute process")
    } else {
        aws_command
            .stdin(zstd_command_output.stdout.unwrap())
            .arg("s3")
            .arg("cp")
            .arg("-")
            .arg(format!("s3://{}/{}", bucket_name, storage_key))
            .output()
            .wrap_err("failed to execute process")
    };
    // ${surreal}/bin/surreal export -e http://${surrealdb.address} -u root -p ${surrealdb.password} --namespace $NS --database calamu - \
    // | ${nixpkgs.zstd}/bin/zstd --force --stdout --adapt --rm - \
    // | ${nixpkgs.awscli}/bin/aws s3 cp - s3://${backupBucket}/$KEY

    let _s3_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", aws_id.clone())
            .env("AWS_SECRET_ACCESS_KEY", aws_key.clone())
            .arg("s3api")
            .arg("put-object-tagging")
            .arg(format!("--endpoint-url {}", aws_endpoint.clone()))
            .arg(format!("--bucket {}", bucket_name))
            .arg(format!("--tagging {}", tags))
            .arg(format!("--key {}", storage_key))
            .output()
            .wrap_err("failed to execute process")?
    } else {
        aws_command
            .arg("s3api")
            .arg("put-object-tagging")
            .arg(format!("--bucket {}", bucket_name))
            .arg(format!("--tagging {}", tags))
            .arg(format!("--key {}", storage_key))
            .output()
            .wrap_err("failed to execute process")?
    };
    // ${nixpkgs.awscli}/bin/aws s3api put-object-tagging \
    // --bucket ${backupBucket} \
    // --tagging "{\"TagSet\":[{\"Key\":\"thirdofhalfday\",\"Value\":\"1\"}$TAGS]}" \
    // --key $KEY
    return s3_cp_command_output;
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
