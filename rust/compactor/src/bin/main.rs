/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use chrono::Local;
use clap::Parser;
use compaction::{merge_sorted_files, CompactionInput};
use human_panic::setup_panic;
use std::{collections::HashMap, io::Write};
use url::Url;

/// Implements a Sleeper compaction algorithm in Rust.
///
/// A sequence of Parquet files is read and compacted into a single output Parquet file. The input
/// files must be individually sorted according to the column numbers specified by `sort_columns`. At least
/// one sort column must be specified and one row key column number. A sketches file containing
/// serialised Apache Data Sketches quantiles sketches are written for reach row key column.
///
#[derive(Parser, Debug)]
#[command(author, version)]
struct CmdLineArgs {
    /// The output file URL for the compacted Parquet data
    output: String,
    /// Set the maximum number of rows in a row group
    #[arg(short = 'r', long, default_value = "1000000")]
    row_group_size: usize,
    /// Set the maximum number of bytes per data page (hint)
    #[arg(short = 'p', long, default_value = "65535")]
    max_page_size: usize,
    /// List of input Parquet files (must be sorted) as URLs
    #[arg(num_args=1.., required=true)]
    input: Vec<String>,
    // Column number for a row key fields
    #[arg(short = 'k', long, default_value = "0")]
    row_keys: Vec<usize>,
    // Column number for sort columns
    #[arg(short = 's', long, default_value = "0")]
    sort_column: Vec<usize>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Install coloured errors
    color_eyre::install().unwrap();

    // Install human readable panics
    setup_panic!();

    // Install and configure environment logger
    env_logger::builder()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] {}:{} - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.file().unwrap_or("??"),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
        .filter_level(log::LevelFilter::Info)
        .format_target(false)
        .init();

    let args = CmdLineArgs::parse();

    // Check URL conversion
    let input_urls = args
        .input
        .iter()
        .map(|x| Url::parse(x).or_else(|_e| Url::parse(&("file://".to_owned() + x))))
        .collect::<Result<Vec<_>, _>>()?;

    // Convert output URL
    let output_url = Url::parse(&args.output)
        .or_else(|_e| Url::parse(&("file://".to_owned() + &args.output)))?;

    let details = CompactionInput {
        input_files: input_urls,
        output_file: output_url,
        max_page_size: args.max_page_size,
        max_row_group_size: args.row_group_size,
        column_truncate_length: 1_048_576,
        stats_truncate_length: 1_048_576,
        compression: "zstd".into(),
        writer_version: "2.0".into(),
        dict_enc_row_keys: true,
        dict_enc_sort_keys: true,
        dict_enc_values: true,
        region: HashMap::default(),
        row_key_cols: vec!["key".into()],
        sort_key_cols: vec![],
    };

    merge_sorted_files(&details).await?;
    Ok(())
}
