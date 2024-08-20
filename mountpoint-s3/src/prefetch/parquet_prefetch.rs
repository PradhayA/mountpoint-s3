use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Deref;

use bytes::{Bytes, BytesMut};
use futures::pin_mut;
use futures::StreamExt;
use intervaltree::IntervalTree;
use mountpoint_s3_client::types::ETag;
use mountpoint_s3_client::ObjectClient;
use parquet::errors::ParquetError;
use parquet::file::footer::decode_footer;
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use tracing::trace;

use super::lru_cache::LruCache;
use super::PrefetchReadError;
use crate::checksums::ChecksummedBytes;
use crate::sync::Arc;

pub use async_lock::RwLock as AsyncRwLock;

const PARQUET_MAGIC_LEN: usize = 8;

/// Wrapper to allow sorting of [Range<u64>].
///
/// Range does not implement [Ord] as there is no generic meaning,
/// however we need it to implement [Ord] to be used in the interval tree.
/// This type considers ranges to be ordered by first comparing the start of the range, then falling back to the end of the range if equal.
#[derive(Debug, Clone)]
pub struct RangeKey(pub Range<u64>);

impl Deref for RangeKey {
    type Target = Range<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for RangeKey {
    fn eq(&self, other: &Self) -> bool {
        // Ranges are equal only if both start and end points match exactly
        self.start == other.start && self.end == other.end
    }
}

impl Eq for RangeKey {}

impl Ord for RangeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary comparison on start, secondary on end
        if self.start == other.start {
            self.end.cmp(&other.end)
        } else {
            self.start.cmp(&other.start)
        }
    }
}

impl PartialOrd for RangeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub type ParsedMetadata = IntervalTree<u64, (RowGroupIndex, ColumnIndex)>;
pub type RowGroupIndex = usize;
pub type ColumnIndex = usize;

#[derive(Debug)]
pub struct RawMetadata {
    pub bytes: ChecksummedBytes,
    pub range: Range<u64>,
}

pub type InMemoryCache = Arc<
    AsyncRwLock<
        Option<(
            HashMap<(RowGroupIndex, ColumnIndex), BTreeMap<RangeKey, ChecksummedBytes>>,
            AsyncRwLock<LruCache>,
        )>,
    >,
>; // Note: Put locks on the inside rather than the outside to allow concurrenta access to the InMemoryCache

/// Read Parquet metadata using the given S3 client,
/// returning the raw bytes and the byte range containing the footer.
pub async fn read_parquet_metadata<Client>(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    object_size: u64,
) -> Result<(Bytes, std::ops::Range<u64>), PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    fetch_metadata(client, bucket, key, if_match, object_size)
        .await
        .map(|(metadata, footer, metadata_range)| {
            let mut combined = BytesMut::with_capacity(metadata.len() + PARQUET_MAGIC_LEN);
            combined.extend_from_slice(&metadata);
            combined.extend_from_slice(&footer);
            (
                combined.freeze(),
                metadata_range.start..(metadata_range.end + PARQUET_MAGIC_LEN as u64),
            )
        })
        .map_err(|_| PrefetchReadError::MetadataParsingFailed)
}

async fn fetch_metadata<Client>(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    object_size: u64,
) -> Result<(Bytes, [u8; PARQUET_MAGIC_LEN], std::ops::Range<u64>), ParquetError>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    if object_size < PARQUET_MAGIC_LEN as u64 {
        return Err(ParquetError::General(
            "Invalid Parquet file. Size is smaller than footer".to_string(),
        ));
    }

    // We refer to the last 8 bytes of a Parquet file as the footer.
    // This is sometimes described differently in different Parquet implementations.
    let mut footer = [0_u8; PARQUET_MAGIC_LEN];
    let footer_range = (object_size - PARQUET_MAGIC_LEN as u64)..object_size;

    fetch_object_part(&client, bucket, key, if_match, footer_range.clone(), |body| {
        footer.copy_from_slice(&body[..PARQUET_MAGIC_LEN])
    })
    .await?;

    let metadata_len = decode_footer(&footer)?;
    let file_metadata_len = PARQUET_MAGIC_LEN + metadata_len;

    if file_metadata_len > object_size as usize {
        return Err(ParquetError::General(
            "Invalid Parquet file. Reported metadata length is shorter than expected".to_string(),
        ));
    }

    let metadata_start = object_size - file_metadata_len as u64;
    let metadata_range = metadata_start..object_size - PARQUET_MAGIC_LEN as u64;

    let mut metadata = BytesMut::with_capacity(metadata_len);
    fetch_object_part(&client, bucket, key, if_match, metadata_range.clone(), |body| {
        metadata.extend_from_slice(body)
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

/// Parse metadata to build a mapping from between byte offsets and the row group and column it belongs to.
///
/// Stored as an interval tree, where the key is the start and end byte of the column chunk, and the value is the rowgroup and column index
/// This allows us to efficiently find the column chunk for a given byte offset
/// The value is stored as a tuple of (rowgroup_index, column_index) to allow for easy retrieval of the column metadata
/// Overall, this approach has a time complexity of O(n log n) for constructing the tree, and O(log n) for lookup, resulting in an efficient solution for finding the column chunk for a given byte offset
pub fn parse_byte_ranges_tree(
    metadata: &ParquetMetaData,
) -> (
    IntervalTree<u64, (RowGroupIndex, ColumnIndex)>,
    HashMap<(RowGroupIndex, ColumnIndex), Range<u64>>,
) {
    let mut elements = Vec::new();
    let mut rowgroup_col_ranges = HashMap::new();

    for (rowgroup_index, rowgroup) in metadata.row_groups().iter().enumerate() {
        for (column_index, column_metadata) in rowgroup.columns().iter().enumerate() {
            let start_byte = column_metadata.file_offset() as u64;
            let end_byte = start_byte + column_metadata.compressed_size() as u64;
            elements.push((start_byte..end_byte, (rowgroup_index, column_index)));
            rowgroup_col_ranges.insert((rowgroup_index, column_index), start_byte..end_byte);
        }
    }

    let interval_tree = IntervalTree::from_iter(elements);
    (interval_tree, rowgroup_col_ranges)
}
