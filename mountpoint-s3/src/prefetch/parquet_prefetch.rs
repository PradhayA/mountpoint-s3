use crate::sync::Arc;
use bytes::{Bytes, BytesMut};
use futures::pin_mut;
use futures::StreamExt;
use intervaltree::IntervalTree;
use mountpoint_s3_client::types::ETag;
use mountpoint_s3_client::ObjectClient;
use parquet::errors::ParquetError;
use parquet::file::footer::decode_footer;
use parquet::file::metadata::ParquetMetaData;
use tracing::trace;

use super::PrefetchReadError;

const PARQUET_MAGIC_LEN: usize = 8;

pub async fn read_parquet_metadata<Client>(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    total_size: u64,
) -> Result<(Bytes, std::ops::Range<u64>), PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    fetch_metadata(client, bucket, key, if_match, total_size)
        .await
        .map(|(metadata, footer, metadata_range)| {
            let mut combined = BytesMut::with_capacity(metadata.len() + PARQUET_MAGIC_LEN);
            combined.extend_from_slice(&metadata);
            combined.extend_from_slice(&footer);
            (combined.freeze(), metadata_range)
        })
        .map_err(|_| PrefetchReadError::MetadataParsingFailed)
}

async fn fetch_metadata<Client>(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    total_size: u64,
) -> Result<(Bytes, [u8; PARQUET_MAGIC_LEN], std::ops::Range<u64>), ParquetError>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    if total_size < PARQUET_MAGIC_LEN as u64 {
        return Err(ParquetError::General(
            "Invalid Parquet file. Size is smaller than footer".to_string(),
        ));
    }

    let mut footer = [0_u8; PARQUET_MAGIC_LEN];
    let footer_range = (total_size - PARQUET_MAGIC_LEN as u64)..total_size;

    fetch_object_part(&client, bucket, key, if_match, footer_range.clone(), |body| {
        footer.copy_from_slice(&body[..PARQUET_MAGIC_LEN])
    })
    .await?;

    let metadata_len = decode_footer(&footer)?;
    let footer_metadata_len = PARQUET_MAGIC_LEN + metadata_len;

    if footer_metadata_len > total_size as usize {
        return Err(ParquetError::General(
            "Invalid Parquet file. Reported metadata length is shorter than expected".to_string(),
        ));
    }

    let metadata_start = total_size - footer_metadata_len as u64;
    let metadata_range = metadata_start..total_size;

    let mut metadata = BytesMut::new();
    fetch_object_part(&client, bucket, key, if_match, metadata_range.clone(), |body| {
        metadata.extend_from_slice(&body)
    })
    .await?;

    Ok((metadata.freeze(), footer, metadata_range))
}

async fn fetch_object_part<Client, F>(
    client: &Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    range: std::ops::Range<u64>,
    mut handle_body: F,
) -> Result<(), ParquetError>
where
    Client: ObjectClient + Send + Sync + 'static,
    F: FnMut(&[u8]),
{
    let get_object_result = client
        .get_object(bucket, key, Some(range), Some(if_match.clone()))
        .await
        .map_err(|e| ParquetError::General(format!("Error fetching object part: {}", e)))?;

    pin_mut!(get_object_result);

    while let Some(result) = get_object_result.next().await {
        match result {
            Ok((_, body)) => {
                trace!(length = body.len(), "received part");
                metrics::counter!("s3.client.total_bytes", "type" => "read").increment(body.len() as u64);
                handle_body(&body);
            }
            Err(e) => return Err(ParquetError::General(format!("Error fetching object part: {}", e))),
        }
    }

    Ok(())
}

pub fn parse_byte_ranges_tree(metadata: &ParquetMetaData) -> IntervalTree<u64, (usize, usize)> {
    // Stored as an interval tree, where the key is the start and end byte of the column chunk, and the value is the rowgroup and column index
    // This allows us to efficiently find the column chunk for a given byte offset
    // The value is stored as a tuple of (rowgroup_index, column_index) to allow for easy retrieval of the column metadata
    // Overall, this approach has a time complexity of O(n log n) for constructing the tree, and O(log n) for lookup, resulting in an efficient solution for finding the column chunk for a given byte offset

    let elements = metadata
        .row_groups()
        .iter()
        .enumerate()
        .flat_map(|(rowgroup_index, rowgroup)| {
            rowgroup
                .columns()
                .iter()
                .enumerate()
                .map(move |(column_index, column_metadata)| {
                    let start_byte = column_metadata.file_offset() as u64;
                    let end_byte = start_byte + column_metadata.compressed_size() as u64 - 1;
                    (start_byte..end_byte, (rowgroup_index, column_index))
                })
        })
        .collect::<Vec<_>>();

    IntervalTree::from_iter(elements)
}

pub fn get_row_groups_and_columns(
    interval_tree: &IntervalTree<u64, (usize, usize)>,
    start_byte: u64,
    end_byte: u64,
) -> Vec<((usize, usize), (u64, u64))> {
    interval_tree
        .query(start_byte..end_byte)
        .map(|element| {
            let intersection_start = start_byte.max(element.range.start);
            let intersection_end = end_byte.min(element.range.end);
            (element.value, (intersection_start, intersection_end))
        })
        .collect()
}
