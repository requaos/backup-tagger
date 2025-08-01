use chrono::{DateTime, Duration, Utc};
use clap::{command, error, Parser, Subcommand};
use color_eyre::eyre::{ContextCompat, Result};
use color_eyre::{eyre::Report, eyre::WrapErr, Section};
use cron_parser::parse;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Output, Stdio};
use tracing::{info, instrument};
use valuable::Valuable;

/// Backup TiKV/SurrealDB S3 Tags
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    /// Matching every n hours
    #[arg(short = 'n', long, default_value_t = 4, global=true)]
    every_n_hours: i64,

    /// Minutes forward from the top of the hour to offset match by
    #[arg(short, long, default_value_t = 30, global=true)]
    minutes_offset_from_hour: i64,

    /// Hours forward from midnight to offset match by
    #[arg(short = 'h', long, default_value_t = 0, global=true)]
    day_offset_in_hours: i64,

    /// Matching window for clock skew and/or job trigger delay
    #[arg(short, long, default_value_t = 20, global=true)]
    lag_window_in_minutes: i64,

    /// Storage key timestamp format string
    #[arg(short, long, default_value_t = String::from("+%Y-%m-%d.%H-%M"), global=true)]
    format_timestamp: String,

    /// Path containing 'bin/aws', 'bin/zstd', 'bin/surreal' and 'bin/tikv-br'.
    #[arg(short, long, default_value_t = String::from("/"))]
    bin_path: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// SurrealDB backup command.
    Surrealdb {
        /// Backup target bucket name.
        #[arg(short = 'B', long)]
        bucket_name: String,

        /// S3 service endpoint address. Leave unspecified to use host defaults.
        #[arg(short = 'e', long)]
        aws_endpoint: String,

        /// S3 access key ID. Leave unspecified to use host defaults.
        #[arg(short = 'i', long)]
        aws_id: String,

        /// S3 secret access Key. Leave unspecified to use host defaults.
        #[arg(short = 'k', long)]
        aws_key: String,

        /// SurrealDB namespace to backup.
        #[arg(short = 'N', long)]
        namespace: String,

        /// SurrealDB database to backup.
        #[arg(short, long)]
        database: String,

        /// SurrealDB server address.
        #[arg(short, long)]
        address: String,

        /// SurrealDB server password.
        #[arg(short, long)]
        password: String,
    },
    /// TiKV backup command.
    Tikv {
        /// Backup target bucket name.
        #[arg(short = 'B', long)]
        bucket_name: String,

        /// S3 service endpoint address. Leave unspecified to use host defaults.
        #[arg(short = 'e', long)]
        aws_endpoint: String,

        /// S3 access key ID. Leave unspecified to use host defaults.
        #[arg(short = 'i', long)]
        aws_id: String,

        /// S3 secret access Key. Leave unspecified to use host defaults.
        #[arg(short = 'k', long)]
        aws_key: String,

        /// TiKV placement driver address: '{host}:{port}'.
        #[arg(short, long)]
        pd_host_and_port: String,
    },
    /// Just print the tags.
    Tags,
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
    let checks = periods(
        args.day_offset_in_hours,
        args.minutes_offset_from_hour,
        args.every_n_hours,
    );

    let now = Utc::now();
    info!(
        "Capturing current UTC time and adjusting within lag window: {}",
        now.to_rfc3339()
    );
    // Need to subtract a few minutes to catch the current trigger.
    // 1/4 of the lag window feels right.
    let now_comparison_value = now
        .checked_sub_signed(Duration::minutes(args.lag_window_in_minutes / 4))
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
            let next_when = if check.2 {
                next.checked_sub_signed(Duration::days(1))
                    .wrap_err("Unable to adjust next matching run time for period end")
                    .suggestion("Check the system clock")?
            } else {
                next
            };
            let diff = next_when - now;
            if diff.num_seconds().abs() < (args.lag_window_in_minutes * 60) {
                tags.push(check.1.clone());
            }
            info!(target: "match_attempt_results", tag = check.1.as_value(), when = next_when.to_rfc3339(), matched = diff.num_seconds().abs() < args.lag_window_in_minutes);
        }
    }
    let tag_set_string = serde_json::to_string(&TagSet { tag_set: tags })?;
    info!(tag_set_string);

    match args.command {
        Commands::Surrealdb {bucket_name, aws_endpoint, aws_id, aws_key, namespace, database, address, password } => {
            // Check for S3 override parameters, ie- MinIO.
            let s3_endpoint = if aws_endpoint.trim().is_empty() || aws_id.trim().is_empty() || aws_key.trim().is_empty() { 
                None 
            } else { Some((aws_endpoint, aws_id, aws_key))};
            // Command::new will thow if the required binaries do not exist.
            let command_output = surrealdb_backup(now, args.bin_path, bucket_name, namespace, database, address, password, tag_set_string, s3_endpoint, args.format_timestamp)?;
            info!(target: "surrealdb_backup_output", success=command_output.status.success(), exit_code=command_output.status.code().or(Some(0)), stdout=String::from_utf8(command_output.stdout)?, stderr=String::from_utf8(command_output.stderr)?);
        }
        Commands::Tikv {bucket_name, aws_endpoint, aws_id, aws_key, pd_host_and_port } => {
            // Check for S3 override parameters, ie- MinIO.
            let s3_endpoint = if aws_endpoint.trim().is_empty() || aws_id.trim().is_empty() || aws_key.trim().is_empty() { 
                None 
            } else { Some((aws_endpoint, aws_id, aws_key))};
            // Command::new will thow if the required binaries do not exist.
            tikv_backup(now, args.bin_path, bucket_name, pd_host_and_port, tag_set_string, s3_endpoint, args.format_timestamp)?;
        }
        Commands::Tags => {
            print!("{}", tag_set_string);
        }
    }
    Ok(())
}

fn periods(
    day_offset_in_hours: i64,
    minutes_offset_from_hour: i64,
    every_n_hours: i64,
) -> Vec<(String, Tag, bool)> {
    // always tag as standard, so manual runs get tagged for lifecycle rules
    // let standard = (
    //     format!("{} {}/{} * * *", minutes_offset_from_hour, day_offset_in_hours, every_n_hours),
    //     Tag {
    //         key: String::from("standard"),
    //         value: String::from("1"),
    //     },
    //     false,
    // );

    return vec![
        (
            format!(
                "{} {} * * *",
                minutes_offset_from_hour,
                every_n_hours + day_offset_in_hours
            ),
            Tag {
                key: String::from("nightly"),
                value: String::from("1"),
            },
            false,
        ),
        (
            format!(
                "{} {} * * 6",
                minutes_offset_from_hour,
                every_n_hours + day_offset_in_hours
            ),
            Tag {
                key: String::from("weekly"),
                value: String::from("1"),
            },
            false,
        ),
        (
            format!(
                "{} {} 1 * *",
                minutes_offset_from_hour,
                every_n_hours + day_offset_in_hours
            ),
            Tag {
                key: String::from("monthly"),
                value: String::from("1"),
            },
            true,
        ),
        (
            format!(
                "{} {} 1 */3 *",
                minutes_offset_from_hour,
                every_n_hours + day_offset_in_hours
            ),
            Tag {
                key: String::from("quarterly"),
                value: String::from("1"),
            },
            true,
        ),
        (
            format!(
                "{} {} 1 1 *",
                minutes_offset_from_hour,
                every_n_hours + day_offset_in_hours
            ),
            Tag {
                key: String::from("yearly"),
                value: String::from("1"),
            },
            true,
        ),
    ];
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

fn tikv_backup(
    time: DateTime<Utc>,
    bin_path: String,
    bucket_name: String,
    pd_host_and_port: String,
    tags: String,
    s3_endpoint: Option<(String, String, String)>,
    format_string: String,
) -> Result<String, Report> {
    let storage_key = format!("tikv/{}", time.format(format_string.as_str()));
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
    let _s3_create_bucket_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", &aws_id)
            .env("AWS_SECRET_ACCESS_KEY", &aws_key)
            .arg("s3api")
            .arg("create-bucket")
            .arg("--endpoint-url").arg(&aws_endpoint)
            .arg("--bucket").arg(&bucket_name)
            .arg("--output").arg("json")
            .output()
            .unwrap_or_else(|err| {
                info!("Error executing command: {}", err);
                // Return a default or empty Output struct to continue
                std::process::Output {
                    status: std::process::ExitStatus::from_raw(1), // Example error status
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }
            })
    } else {
        aws_command
            .arg("s3api")
            .arg("create-bucket")
            .arg("--bucket").arg(&bucket_name)
            .arg("--output").arg("json")
            .output()
            .unwrap_or_else(|err| {
                info!("Error executing command: {}", err);
                // Return a default or empty Output struct to continue
                std::process::Output {
                    status: std::process::ExitStatus::from_raw(1), // Example error status
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }
            })
    };
    // We want to pass in the TiKV PD address and port
    // may need to pass endpoint address like this: --s3.endpoint http://xxx
    let tikv_br_command_result = if endpoint_is_some {
        Command::new(format!("{}/bin/tikv-br", bin_path))
            .arg("backup")
            .arg("raw")
            .arg(format!("--pd={}", pd_host_and_port))
            .arg(format!("--send-credentials-to-tikv={}", endpoint_is_some))
            .arg(format!("--s3.endpoint={}", &aws_endpoint))
            .arg(format!("--storage=s3://{}/{}?access-key={}&secret-access-key={}", bucket_name, storage_key, aws_id, aws_key))
            .output()
            .wrap_err("failed to execute process")?
        } else {
        Command::new(format!("{}/bin/tikv-br", bin_path))
            .arg("backup")
            .arg("raw")
            .arg(format!("--pd={}", pd_host_and_port))
            .arg(format!("--send-credentials-to-tikv={}", endpoint_is_some))
            .arg(format!("--storage=s3://{}/{}", bucket_name, storage_key))
            .output()
            .wrap_err("failed to execute process")?
        };
    
    let tikv_br_stdout = String::from_utf8(tikv_br_command_result.stdout)?;
    info!(target: "tikv_backup_output", success=tikv_br_command_result.status.success(), exit_code=tikv_br_command_result.status.code().or(Some(0)), stdout=tikv_br_stdout, stderr=String::from_utf8(tikv_br_command_result.stderr)?);

    let s3_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", &aws_id)
            .env("AWS_SECRET_ACCESS_KEY", &aws_key)
            .arg("s3api")
            .arg("list-objects")
            .arg("--endpoint-url").arg(&aws_endpoint)
            .arg("--bucket").arg(&bucket_name)
            .arg("--prefix").arg(&storage_key)
            .arg("--output").arg("json")
            .output()
            .wrap_err("failed to execute process")?
    } else {
        aws_command
            .arg("s3api")
            .arg("list-objects")
            .arg("--bucket").arg(&bucket_name)
            .arg("--prefix").arg(&storage_key)
            .arg("--output").arg("json")
            .output()
            .wrap_err("failed to execute process")?
    };
    // TODO: list all the files that were pushed up by the distributed backup command.
    // LIST_RESP=`${nixpkgs.awscli}/bin/aws s3api list-objects --bucket ${backupBucket} --prefix $KEY --output json`
    let list_response = String::from_utf8(s3_command_output.stdout.clone())?;
    info!(target: "aws_list_objects_output", success=s3_command_output.status.success(), exit_code=s3_command_output.status.code().or(Some(0)), stdout=list_response, stderr=String::from_utf8(s3_command_output.stderr)?);

    let list_object_result = serde_json::from_str::<ListObjectResult>(list_response.as_str())?;
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
            .arg("--bucket").arg(&bucket_name)
            .arg("--tagging").arg(&tags)
            .arg("--key").arg(&key)
            .output()
            .wrap_err("failed to execute process")?;
        info!(target: "aws_put_object_tagging_output", key=key, success=_s3_command_output.status.success(), exit_code=_s3_command_output.status.code().or(Some(0)), stdout=String::from_utf8(_s3_command_output.stdout)?, stderr=String::from_utf8(_s3_command_output.stderr)?);
    }
    // TODO: Apply tags to all keys returned from list operation.
    // ${nixpkgs.findutils}/bin/xargs -rP 4 -n 1 ${nixpkgs.awscli}/bin/aws s3api put-object-tagging \
    // --bucket ${backupBucket} \
    // --tagging "{\"TagSet\":[{\"Key\":\"thirdofhalfday\",\"Value\":\"1\"}$TAGS]}" \
    // --key <<< "$KEYS"

    return Ok(tikv_br_stdout);
}

fn surrealdb_backup(
    time: DateTime<Utc>,
    bin_path: String,
    bucket_name: String,
    namespace: String,
    database: String,
    address: String,
    password: String,
    tags: String,
    s3_endpoint: Option<(String, String, String)>,
    format_string: String,
) -> Result<Output, Report> {
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
    // Create bucket if not exists, ignore errors.
    let _s3_create_bucket_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", &aws_id)
            .env("AWS_SECRET_ACCESS_KEY", &aws_key)
            .arg("s3api")
            .arg("create-bucket")
            .arg("--endpoint-url").arg(&aws_endpoint)
            .arg("--bucket").arg(&bucket_name)
            .arg("--output").arg("json")
            .output()
            .unwrap_or_else(|err| {
                info!("Error executing command: {}", err);
                // Return a default or empty Output struct to continue
                std::process::Output {
                    status: std::process::ExitStatus::from_raw(1), // Example error status
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }
            })
    } else {
        aws_command
            .arg("s3api")
            .arg("create-bucket")
            .arg("--bucket").arg(&bucket_name)
            .arg("--output").arg("json")
            .output()
            .unwrap_or_else(|err| {
                info!("Error executing command: {}", err);
                // Return a default or empty Output struct to continue
                std::process::Output {
                    status: std::process::ExitStatus::from_raw(1), // Example error status
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }
            })
    };
    let time_part = time.format(format_string.as_str()).to_string().replace("+", "");
    let storage_key = format!("surrealdb/{}/{}.zst", namespace, time_part);
    // KEY=surrealdb/$NS/${ds}.zst

    let mut s3_cp_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", aws_id.clone())
            .env("AWS_SECRET_ACCESS_KEY", aws_key.clone())
            .stdin(Stdio::piped())
            .arg("s3")
            .arg("cp")
            .arg("--endpoint-url").arg(aws_endpoint.clone())
            .arg("-")
            .arg(format!("s3://{}/{}", bucket_name, storage_key))
            .spawn()
            .wrap_err("failed to execute process")
    } else {
        aws_command
            .stdin(Stdio::piped())
            .arg("s3")
            .arg("cp")
            .arg("-")
            .arg(format!("s3://{}/{}", bucket_name, storage_key))
            .spawn()
            .wrap_err("failed to execute process")
    }?;
    let mut zstd_command_output = Command::new(format!("{}/bin/zstd", bin_path))
        .stdin(Stdio::piped())
        .arg("--force")
        .arg("--stdout")
        .arg("--adapt")
        .arg("--rm")
        .arg("-")
        .stdout(s3_cp_command_output.stdin.take().wrap_err("failed to pipe")?)
        .spawn()
        .wrap_err("failed to execute process")?;
    let surrealdb_command_output = Command::new(format!("{}/bin/surreal", bin_path))
        .arg("export")
        .arg("-e").arg(format!("http://{}", address))
        .arg("-u").arg("root")
        .arg("-p").arg(password)
        .arg("--namespace").arg(namespace)
        .arg("--database").arg(database)
        .arg("-").stdout(zstd_command_output.stdin.take().wrap_err("failed to pipe")?)
        .spawn()
        .wrap_err("failed to execute process")?;
    let s3_command_output = s3_cp_command_output.wait_with_output().wrap_err("failed to wait for the piped run")?;
    info!("{}", String::from_utf8(surrealdb_command_output.wait_with_output()?.stderr)?);
    // ${surreal}/bin/surreal export -e http://${surrealdb.address} -u root -p ${surrealdb.password} --namespace $NS --database calamu - \
    // | ${nixpkgs.zstd}/bin/zstd --force --stdout --adapt --rm - \
    // | ${nixpkgs.awscli}/bin/aws s3 cp - s3://${backupBucket}/$KEY

    let _s3_command_output = if endpoint_is_some {
        aws_command
            .env("AWS_ACCESS_KEY_ID", aws_id.clone())
            .env("AWS_SECRET_ACCESS_KEY", aws_key.clone())
            .arg("s3api")
            .arg("put-object-tagging")
            .arg("--endpoint-url").arg(aws_endpoint.clone())
            .arg("--bucket").arg(bucket_name)
            .arg("--tagging").arg(tags)
            .arg("--key").arg(storage_key)
            .output()
            .wrap_err("failed to execute process")?
    } else {
        aws_command
            .arg("s3api")
            .arg("put-object-tagging")
            .arg("--bucket").arg(bucket_name)
            .arg("--tagging").arg(tags)
            .arg("--key").arg(storage_key)
            .output()
            .wrap_err("failed to execute process")?
    };
    info!("{}", String::from_utf8(_s3_command_output.stdout)?);
    // ${nixpkgs.awscli}/bin/aws s3api put-object-tagging \
    // --bucket ${backupBucket} \
    // --tagging "{\"TagSet\":[{\"Key\":\"thirdofhalfday\",\"Value\":\"1\"}$TAGS]}" \
    // --key $KEY
    return Ok(s3_command_output);
}

fn install_tracing() {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{fmt, EnvFilter};

    let fmt_layer = fmt::layer().with_writer(|| std::io::stderr()).with_target(false);
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(ErrorLayer::default())
        .init();
}
