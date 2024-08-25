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
use super::RowgroupColRanges;
use crate::checksums::ChecksummedBytes;
use crate::sync::Arc;

pub use async_lock::RwLock as AsyncRwLock;

const METADATA_READ_SIZE: u64 = 8192; // Estimate of max metadata footer size

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

pub type CachedRanges = BTreeMap<RangeKey, ChecksummedBytes>;
pub type InMemoryCache = HashMap<(RowGroupIndex, ColumnIndex), CachedRanges>;
pub type LruCacheRef = AsyncRwLock<LruCache>;

#[derive(Debug)]
pub struct InMemoryRecord {
    pub in_memory_cache: InMemoryCache,
    pub lru_cache: LruCacheRef,
}

pub type InMemoryCacheRef = Arc<AsyncRwLock<Option<InMemoryRecord>>>;

/// Read Parquet metadata using the given S3 client,
/// returning the raw bytes and the byte range containing the footer.
pub async fn read_parquet_metadata<Client>(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    object_size: u64,
) -> Result<(Bytes, std::ops::Range<u64>, u64), PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    let read_size = METADATA_READ_SIZE.min(object_size);
    let raw_metadata_range_end = object_size;
    let raw_metadata_range_start = raw_metadata_range_end.saturating_sub(read_size);
    let raw_metadata = fetch_object_part(
        &client,
        bucket,
        key,
        if_match,
        raw_metadata_range_start..raw_metadata_range_end,
    )
    .await
    .map_err(|_| PrefetchReadError::MetadataParsingFailed)?;

    let metadata_len = {
        let footer_bytes: [u8; 8] = raw_metadata[raw_metadata.len() - 8..]
            .try_into()
            .map_err(|_| PrefetchReadError::MetadataParsingFailed)?;
        decode_footer(&footer_bytes).map_err(|_| PrefetchReadError::MetadataParsingFailed)?
    };

    let metadata_start = object_size - metadata_len as u64 - 8;
    if metadata_start < raw_metadata_range_start {
        // Metadata is partially outside the read range, read the remaining bytes
        let remaining_metadata =
            match fetch_object_part(&client, bucket, key, if_match, metadata_start..raw_metadata_range_start).await {
                Ok(bytes) => bytes,
                Err(_) => return Err(PrefetchReadError::MetadataParsingFailed),
            };

        let total_metadata_len = remaining_metadata.len() + raw_metadata.len();
        let mut raw_metadata_bytes = BytesMut::with_capacity(total_metadata_len);
        raw_metadata_bytes.extend_from_slice(&remaining_metadata[..]);
        raw_metadata_bytes.extend_from_slice(&raw_metadata);
        Ok((
            raw_metadata_bytes.freeze(),
            metadata_start..raw_metadata_range_end,
            metadata_start,
        ))
    } else {
        Ok((
            raw_metadata,
            raw_metadata_range_start..raw_metadata_range_end,
            metadata_start,
        ))
    }
}

async fn fetch_object_part<Client>(
    client: &Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    range: std::ops::Range<u64>,
) -> Result<Bytes, ParquetError>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    let mut body = BytesMut::new();

    let get_object_result = client
        .get_object(bucket, key, Some(range), Some(if_match.clone()))
        .await
        .map_err(|e| ParquetError::General(format!("Error fetching object part: {}", e)))?;

    pin_mut!(get_object_result);

    while let Some(result) = get_object_result.next().await {
        match result {
            Ok((_, bytes)) => {
                trace!(length = bytes.len(), "received part");
                metrics::counter!("s3.client.total_bytes", "type" => "read").increment(bytes.len() as u64);
                body.extend_from_slice(&bytes);
            }
            Err(e) => return Err(ParquetError::General(format!("Error fetching object part: {}", e))),
        }
    }

    Ok(body.freeze())
}

/// Parse metadata to build a mapping from between byte offsets and the row group and column it belongs to.
///
/// Stored as an interval tree, where the key is the start and end byte of the column chunk, and the value is the rowgroup and column index
/// This allows us to efficiently find the column chunk for a given byte offset
/// The value is stored as a tuple of (rowgroup_index, column_index) to allow for easy retrieval of the column metadata
/// Overall, this approach has a time complexity of O(n log n) for constructing the tree, and O(log n) for lookup, resulting in an efficient solution for finding the column chunk for a given byte offset
pub fn parse_byte_ranges_tree(metadata: &ParquetMetaData) -> (ParsedMetadata, RowgroupColRanges) {
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

#[cfg(test)]
mod tests {
    use crate::prefetch::{
        lru_cache::LruCache,
        parquet_prefetch::{CachedRanges, InMemoryCache, InMemoryRecord, RangeKey},
    };
    pub use async_lock::RwLock as AsyncRwLock;

    #[test]
    fn test_range_key() {
        let range1 = RangeKey(0..100);
        let range2 = RangeKey(0..200);
        let range3 = RangeKey(100..200);

        assert!(range1 < range2);
        assert!(range1 < range3);
        assert!(range2 < range3);

        assert_eq!(range1.cmp(&range1), std::cmp::Ordering::Equal);
        assert_eq!(range1.cmp(&range2), std::cmp::Ordering::Less);
        assert_eq!(range2.cmp(&range1), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_in_memory_record() {
        let mut in_memory_cache = InMemoryCache::new();
        in_memory_cache.insert((0, 0), CachedRanges::new());

        let lru_cache = AsyncRwLock::new(LruCache::new(1000));

        let record = InMemoryRecord {
            in_memory_cache,
            lru_cache,
        };

        assert_eq!(record.in_memory_cache.len(), 1);
        assert!(record.in_memory_cache.contains_key(&(0, 0)));
    }
}
