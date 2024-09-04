use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

pub use async_lock::RwLock as AsyncRwLock;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::task::{Spawn, SpawnExt};
use futures::{pin_mut, StreamExt};
use mountpoint_s3_client::{types::ETag, ObjectClient};
use parquet::file::footer::decode_metadata;
use tracing::{debug_span, error, trace, warn, Instrument};

use crate::checksums::{ChecksummedBytes, IntegrityError};
use crate::object::ObjectId;
use crate::prefetch::lru_cache::{CacheKey, LruCache};
use crate::prefetch::parquet_prefetch::{parse_byte_ranges_tree, read_parquet_metadata};
use crate::prefetch::part::Part;
use crate::prefetch::part_queue::{unbounded_part_queue, PartQueueProducer};
use crate::prefetch::part_stream::{ObjectPartStream, RequestRange};
use crate::prefetch::task::RequestTask;
use crate::prefetch::{PrefetchReadError, RawMetadata};

use super::parquet_prefetch::{
    CachedRanges, ColumnIndex, InMemoryCache, InMemoryRecord, LruCacheRef, RangeKey, RowGroupIndex,
};
use super::ParsedMetadata;

type RowgroupCols = Vec<((RowGroupIndex, ColumnIndex), Range<u64>)>;

pub type RowgroupColRanges = HashMap<(RowGroupIndex, ColumnIndex), Range<u64>>;

#[derive(Clone, Debug)]
pub struct MetadataRanges {
    pub parsed_metadata: ParsedMetadata,
    pub rowgroup_col_ranges: RowgroupColRanges,
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub state: Arc<AsyncRwLock<CacheEntryState>>,
}

impl Default for CacheEntry {
    fn default() -> Self {
        Self {
            state: Arc::new(AsyncRwLock::new(CacheEntryState::NotRead)),
        }
    }
}

#[derive(Debug)]
pub struct CacheEntryValue {
    pub raw_metadata: RawMetadata,
    pub parsed_metadata: MetadataRanges,
    pub in_memory_cache: InMemoryRecord,
}

#[derive(Debug)]
pub enum CacheEntryState {
    NotRead,
    ReadSuccessfully { cache_entry_value: CacheEntryValue },
    ParsingFailed,
}

type MetadataCache = DashMap<ObjectId, CacheEntry>;

#[derive(Debug)]
pub struct ParquetPartStream<Runtime> {
    runtime: Runtime,
    cache: Arc<MetadataCache>,
}

impl<Runtime> ParquetPartStream<Runtime> {
    pub fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            cache: Arc::new(MetadataCache::new()),
        }
    }
}

#[async_trait]
impl<Runtime> ObjectPartStream for ParquetPartStream<Runtime>
where
    Runtime: Spawn,
{
    #[allow(clippy::too_many_arguments)]
    fn spawn_get_object_request<Client>(
        &self,
        client: &Client,
        bucket: &str,
        key: &str,
        if_match: ETag,
        range: RequestRange,
        preferred_part_size: usize,
    ) -> RequestTask<Client::ClientError>
    where
        Client: ObjectClient + Clone + Send + Sync + 'static,
    {
        let start = range.start();
        let size = range.len();
        let (part_queue, part_queue_producer) = unbounded_part_queue();

        let request_task = {
            let client = client.clone();
            let bucket = bucket.to_owned();
            let id = ObjectId::new(key.to_owned(), if_match.clone());
            let span = debug_span!("prefetch", range=?range);
            let key = key.to_owned();

            let ref_cache = self
                .cache
                .entry(ObjectId::new(key.to_owned(), if_match.clone()))
                .or_default();

            let cached_entry = ref_cache.value().clone();

            async move {
                // Check if metadata is already stored in the cache, if not, read, parse and store the metadata
                if key.ends_with(".parquet") {
                    let state = cached_entry.state.read().await;
                    match &*state {
                        CacheEntryState::NotRead => {
                            drop(state);
                            let mut state = cached_entry.state.write().await;
                            if let CacheEntryState::NotRead = &*state {
                                let result = parse_parquet_metadata(
                                    client.clone(),
                                    &bucket,
                                    &key,
                                    &if_match,
                                    range.object_size() as u64,
                                )
                                .await;

                                if let Ok(cache_value) = result {
                                    *state = CacheEntryState::ReadSuccessfully {
                                        cache_entry_value: cache_value,
                                    };
                                } else {
                                    *state = CacheEntryState::ParsingFailed;
                                    drop(state);

                                    // Fallback to the previous behavior from part_stream.rs
                                    spawn_default_get_object_request(
                                        client,
                                        bucket,
                                        &key,
                                        if_match,
                                        range,
                                        preferred_part_size,
                                        part_queue_producer,
                                    )
                                    .await;
                                    return;
                                }
                            }
                        }
                        CacheEntryState::ParsingFailed => {
                            // Fallback to the previous behavior from part_stream.rs
                            spawn_default_get_object_request(
                                client,
                                bucket,
                                &key,
                                if_match,
                                range,
                                preferred_part_size,
                                part_queue_producer,
                            )
                            .await;
                            return;
                        }
                        _ => {}
                    }
                } else {
                    // Fallback to the previous behavior from part_stream.rs
                    spawn_default_get_object_request(
                        client,
                        bucket,
                        &key,
                        if_match,
                        range,
                        preferred_part_size,
                        part_queue_producer,
                    )
                    .await;
                    return;
                }

                // If everything is correct i.e. if it's a parquet file and the metadata was loaded properly
                if let CacheEntryState::ReadSuccessfully { cache_entry_value } = &*cached_entry.state.read().await {
                    let request_range = range.start()..range.end();

                    let mut remaining_range = request_range.clone();
                    let mut metadata_part = None;

                    // If range overlaps with the raw metadata, serve from RawMetadata cache
                    if let Some(intersection) =
                        intersect_ranges(&remaining_range, &cache_entry_value.raw_metadata.range)
                    {
                        trace!("Metadata portion detected: fetching from cache");
                        remaining_range = remaining_range.start..intersection.start;

                        let metadata_start = (intersection.start - cache_entry_value.raw_metadata.range.start) as usize;
                        let metadata_end = (intersection.end - cache_entry_value.raw_metadata.range.start) as usize;
                        let metadata_bytes = cache_entry_value.raw_metadata.bytes.slice(metadata_start..metadata_end);
                        trace!("Metadata range start-end: {:?}-{:?}", metadata_start, metadata_end);
                        metadata_part = Some(Part::new(id.clone(), intersection.start, metadata_bytes));
                    }

                    // If range overlaps with the first 4 magic bytes, serve PAR1 straight away
                    if !remaining_range.is_empty() && remaining_range.start < 4 {
                        let overlap_start = remaining_range.start;
                        let overlap_end = std::cmp::min(remaining_range.end, 4);
                        let magic_bytes = ChecksummedBytes::new(Bytes::copy_from_slice(b"PAR1"));
                        let partial = magic_bytes.slice(overlap_start as usize..overlap_end as usize);
                        part_queue_producer.push(Ok(Part::new(id.clone(), overlap_start, partial)));
                        remaining_range = overlap_end..remaining_range.end;
                    }

                    let mut rowgroup_cols: RowgroupCols = get_row_groups_and_columns(
                        &cache_entry_value.parsed_metadata.parsed_metadata,
                        remaining_range.clone(),
                    );

                    while !remaining_range.is_empty() {
                        let start = Instant::now();
                        let cache_read_success = try_serve_from_cache(
                            &mut remaining_range,
                            &cache_entry_value.in_memory_cache,
                            &id,
                            &part_queue_producer,
                            &rowgroup_cols,
                        )
                        .await;
                        trace!("Let elapsed {:?}", start.elapsed());
                        let hist = metrics::histogram!("serve_from_cache");
                        hist.record(start.elapsed().as_micros() as f64);

                        if !cache_read_success {
                            let size = remaining_range.end - remaining_range.start; // or let size = preferred_part_size; (needs more research)
                            if let Err(e) = fetch_from_client(
                                &client,
                                &bucket,
                                &id,
                                &mut remaining_range,
                                size as usize,
                                &cache_entry_value.in_memory_cache,
                                &mut rowgroup_cols,
                                &part_queue_producer,
                                &cache_entry_value.parsed_metadata,
                            )
                            .await
                            {
                                error!("Error fetching from client: {:?}", e);
                                part_queue_producer.push(Err(e));
                                return;
                            }
                        }
                    }

                    if let Some(metadata_part) = metadata_part {
                        part_queue_producer.push(Ok(metadata_part));
                    }
                } else {
                    // Fallback to the previous behavior from part_stream.rs
                    spawn_default_get_object_request(
                        client,
                        bucket,
                        &key,
                        if_match,
                        range,
                        preferred_part_size,
                        part_queue_producer,
                    )
                    .await;
                }

                trace!("request finished");
            }
            .instrument(span)
        };

        let task_handle = self.runtime.spawn_with_handle(request_task).unwrap();
        RequestTask::from_handle(task_handle, size, start, part_queue)
    }
}

// Parse parquet metadata
async fn parse_parquet_metadata<Client>(
    client: Client,
    bucket: &str,
    key: &str,
    if_match: &ETag,
    object_size: u64,
) -> Result<CacheEntryValue, PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    let (raw_metadata, raw_metadata_range, metadata_start) =
        read_parquet_metadata(client.clone().into(), bucket, key, if_match, object_size)
            .await
            .map_err(PrefetchReadError::from)?;

    let metadata_len = raw_metadata.len() - 8;
    let metadata_area = metadata_start - raw_metadata_range.start;

    let metadata = decode_metadata(&raw_metadata[metadata_area as usize..metadata_len])
        .map_err(|_| PrefetchReadError::<Client::ClientError>::MetadataParsingFailed)?;

    let raw_metadata = RawMetadata {
        bytes: ChecksummedBytes::new(raw_metadata),
        range: raw_metadata_range,
    };

    let (parsed_metadata, rowgroup_col_ranges) = parse_byte_ranges_tree(&metadata);

    let parsed_metadata = MetadataRanges {
        parsed_metadata,
        rowgroup_col_ranges,
    };

    let in_memory_cache = InMemoryRecord {
        in_memory_cache: Default::default(),
        lru_cache: AsyncRwLock::new(LruCache::new(1000 * 1024 * 1024)), // 1 GB limit
    };

    Ok(CacheEntryValue {
        raw_metadata,
        parsed_metadata,
        in_memory_cache,
    })
}

/// Default spawn object request as in default_stream
async fn spawn_default_get_object_request<Client>(
    client: Client,
    bucket: String,
    key: &str,
    if_match: ETag,
    range: RequestRange,
    preferred_part_size: usize,
    part_queue_producer: PartQueueProducer<Client::ClientError>,
) where
    Client: ObjectClient + Clone + Send + Sync + 'static,
{
    trace!("Spawning default get object request");
    assert!(preferred_part_size > 0);
    let request_range = range.align(client.part_size().unwrap_or(8 * 1024 * 1024) as u64, true);

    trace!(range=?request_range, "spawning request");
    let get_object_result = match client
        .get_object(&bucket, key, Some(request_range.into()), Some(if_match.clone()))
        .await
    {
        Ok(get_object_result) => get_object_result,
        Err(e) => {
            error!(key=key, error=?e, "GetObject request failed");
            part_queue_producer.push(Err(PrefetchReadError::GetRequestFailed(e)));
            return;
        }
    };

    pin_mut!(get_object_result);
    loop {
        match get_object_result.next().await {
            Some(Ok((offset, body))) => {
                trace!(offset, length = body.len(), "received GetObject part");
                metrics::counter!("s3.client.total_bytes", "type" => "read").increment(body.len() as u64);
                // pre-split the body into multiple parts as suggested by preferred part size
                // in order to avoid validating checksum on large parts at read.
                let mut body: Bytes = body.into();
                let mut curr_offset = offset;
                loop {
                    let chunk_size = preferred_part_size.min(body.len());
                    if chunk_size == 0 {
                        break;
                    }
                    let chunk = body.split_to(chunk_size);
                    // S3 doesn't provide checksum for us if the request range is not aligned to
                    // object part boundaries, so we're computing our own checksum here.
                    let checksum_bytes = ChecksummedBytes::new(chunk);
                    let part = Part::new(
                        ObjectId::new(key.to_owned(), if_match.clone()),
                        curr_offset,
                        checksum_bytes,
                    );
                    curr_offset += part.len() as u64;
                    part_queue_producer.push(Ok(part));
                }
            }
            Some(Err(e)) => {
                error!(key=key, error=?e, "GetObject body part failed");
                part_queue_producer.push(Err(PrefetchReadError::GetRequestFailed(e)));
                break;
            }
            None => break,
        }
    }
    trace!("request finished");
}

/// Tries to serve data from cache as much as possible
async fn try_serve_from_cache<E: std::error::Error + Send + Sync + 'static>(
    remaining_range: &mut Range<u64>,
    in_mem_record: &InMemoryRecord,
    id: &ObjectId,
    part_queue_producer: &PartQueueProducer<E>,
    rowgroup_cols: &RowgroupCols,
) -> bool {
    for (row_group_col, col_range) in rowgroup_cols {
        if let Some(col_cache) = in_mem_record.in_memory_cache.read().await.get(row_group_col) {
            let cached_ranges: Vec<_> = col_cache.keys().cloned().collect();
            if !cached_ranges.is_empty() {
                move_entry_to_back(id, row_group_col, &in_mem_record.lru_cache).await;
                // Still move entry to back since this (rowgroup, col) is accessed
            }
            for cached_range in &cached_ranges {
                if cached_range.start >= col_range.end {
                    metrics::counter!("prefetch.parquet_cache_misses").increment(1);
                    return false;
                } // Return early since no point going forward from here

                if let Some(intersection) = intersect_ranges(cached_range, remaining_range) {
                    let data = col_cache.get(cached_range).unwrap();
                    let part_start = intersection.start;
                    let part_end = intersection.end;
                    let data_offset = part_start - cached_range.start;

                    if part_start == remaining_range.start {
                        let part_data =
                            data.slice(data_offset as usize..(data_offset + (part_end - part_start)) as usize);
                        let part = Part::new(id.clone(), part_start, part_data);

                        trace!("Pushing part to queue from cache: {:?}", part_start..part_end);
                        part_queue_producer.push(Ok(part));

                        *remaining_range = part_end..remaining_range.end;
                        metrics::counter!("prefetch.parquet_cache_hits").increment(1);
                        if remaining_range.is_empty() {
                            return true;
                        }
                    } else {
                        metrics::counter!("prefetch.parquet_cache_misses").increment(1);
                        return false; // Return early since no point going forward from here
                    }
                }
            }
        }
    }
    metrics::counter!("prefetch.parquet_cache_misses").increment(1);
    false
}

/// Moves entry (row group, col) to the back of LRU entry cache
async fn move_entry_to_back(id: &ObjectId, row_group_col: &(usize, usize), lru_cache: &AsyncRwLock<LruCache>) {
    let key = CacheKey {
        file_id: id.clone(),
        row_group: row_group_col.0,
        column: row_group_col.1,
    };
    {
        let mut lru_cache_guard = lru_cache.write().await;
        lru_cache_guard.touch_entry(&key);
    }
}

/// Fetches the requested range from the client
#[allow(clippy::too_many_arguments)]
async fn fetch_from_client<Client>(
    client: &Client,
    bucket: &str,
    id: &ObjectId,
    remaining_range: &mut Range<u64>,
    preferred_part_size: usize,
    in_memory_record: &InMemoryRecord,
    cols: &mut RowgroupCols,
    part_queue_producer: &PartQueueProducer<Client::ClientError>,
    metadata: &MetadataRanges,
) -> Result<(), PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    trace!("Fetching from client for range: {:?}", remaining_range);
    let prefetch_range = RequestRange::new(
        remaining_range.end as usize,
        remaining_range.start,
        (remaining_range.end - remaining_range.start) as usize,
    );

    match get_from_client(client, bucket, id, prefetch_range, preferred_part_size).await {
        Ok(parts) => {
            for part in parts {
                let mut in_memory_cache = in_memory_record.in_memory_cache.write().await;
                let part_range = part.offset()..part.offset() + part.len() as u64;
                // list of row group cols and the ranges they have
                let mut ranges: Vec<(_, _)> = Vec::new();
                for (row_group_col, map) in in_memory_cache.iter() {
                    ranges.push((*row_group_col, map.keys()));
                }

                trace!(
                    "Pushing part to queue from client: {:?} where cache was {:?}",
                    part_range,
                    ranges
                );

                part_queue_producer.push(Ok(part.clone()));

                // Filters out columns so we need to walk through less columns on the next part (next round) leveraging non-overlapping, structured order
                let mut new_cols = Vec::new();
                let mut remaining_cols = Vec::new();

                for (row_group_col, col_range) in cols.iter() {
                    if col_range.start < part_range.end && part_range.start < col_range.end {
                        new_cols.push((*row_group_col, col_range.clone()));
                        if col_range.end > part_range.end {
                            remaining_cols.push((*row_group_col, col_range.clone()));
                        }
                    } else if col_range.start >= part_range.end {
                        remaining_cols.push((*row_group_col, col_range.clone()));
                    }
                }

                *cols = remaining_cols;

                for (row_group_col, _col_range) in new_cols {
                    let col_cache = in_memory_cache.entry(row_group_col).or_insert_with(BTreeMap::new);
                    let col_range = metadata.rowgroup_col_ranges.get(&row_group_col).unwrap();
                    if let Err(e) = merge_ranges(
                        col_cache,
                        part_range.clone(),
                        part.get_checksummed_bytes().clone(),
                        col_range,
                    ) {
                        warn!("Error merging ranges: {:?}", e);
                    }

                    lru_record(
                        id,
                        row_group_col,
                        &part,
                        &in_memory_record.lru_cache,
                        &mut in_memory_cache,
                    )
                    .await;
                }

                *remaining_range = part_range.end..remaining_range.end;
                if remaining_range.is_empty() {
                    break;
                }
            }
        }
        Err(e) => {
            warn!(key=id.key(), error=?e, "GetObject request failed");
            return Err(e);
        }
    }

    Ok(())
}

/// Records entry (rowg roup, col) into LRU entry cache and evict (row group, cols) if necessary
async fn lru_record(
    id: &ObjectId,
    row_group_col: (usize, usize),
    part: &Part,
    lru_cache: &LruCacheRef,
    cache: &mut InMemoryCache,
) {
    let key = CacheKey {
        file_id: id.clone(),
        row_group: row_group_col.0,
        column: row_group_col.1,
    };

    let size = part.len();
    {
        let mut lru_cache_guard = lru_cache.write().await;
        let evicted = lru_cache_guard.add_entry(key, size);
        for evicted_key in evicted {
            if let Some(evicted_col_cache) = cache.get_mut(&(evicted_key.row_group, evicted_key.column)) {
                evicted_col_cache.clear();
            }
        }
    }
}

/// Get from client as requested
async fn get_from_client<Client>(
    client: &Client,
    bucket: &str,
    id: &ObjectId,
    range: RequestRange,
    preferred_part_size: usize,
) -> Result<Vec<Part>, PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    let key = id.key();
    // let block_aligned_byte_range = range;
    let block_aligned_byte_range = range.align(client.part_size().unwrap_or(8 * 1024 * 1024) as u64, true);

    trace!(?key, range =? block_aligned_byte_range, "fetching data from client");

    let get_object_result = client
        .get_object(
            bucket,
            key,
            Some(block_aligned_byte_range.into()),
            Some(id.etag().clone()),
        )
        .await
        .map_err(PrefetchReadError::GetRequestFailed)?;

    pin_mut!(get_object_result);

    let mut parts = Vec::new();

    while let Some(result) = get_object_result.next().await {
        match result {
            Ok((offset, body)) => {
                trace!(offset, length = body.len(), "received GetObject part");
                metrics::counter!("s3.client.total_bytes", "type" => "read").increment(body.len() as u64);
                // pre-split the body into multiple parts as suggested by preferred part size
                // in order to avoid validating checksum on large parts at read.
                let mut body: Bytes = body.into();
                let mut curr_offset = offset;
                loop {
                    let chunk_size = preferred_part_size.min(body.len());
                    if chunk_size == 0 {
                        break;
                    }
                    let chunk = body.split_to(chunk_size);
                    // S3 doesn't provide checksum for us if the request range is not aligned to
                    // object part boundaries, so we're computing our own checksum here.
                    let checksum_bytes = ChecksummedBytes::new(chunk);
                    let part = Part::new(id.clone(), curr_offset, checksum_bytes);
                    curr_offset += part.len() as u64;
                    parts.push(part);
                }
            }
            Err(e) => {
                warn!(key, error=?e, "GetObject body part failed");
                return Err(PrefetchReadError::GetRequestFailed(e));
            }
        }
    }

    trace!("request finished");
    Ok(parts)
}

/// Get row groups and columns that intersect with the given range
///
/// Returns a vector of tuples containing the row group and column index, and the intersecting range
fn get_row_groups_and_columns(interval_tree: &ParsedMetadata, range: Range<u64>) -> RowgroupCols {
    interval_tree
        .iter_sorted()
        .filter_map(|element| {
            let intersection_start = element.range.start.max(range.start);
            let intersection_end = element.range.end.min(range.end);

            if intersection_start < intersection_end {
                Some((element.value, intersection_start..intersection_end))
            } else {
                None
            }
        })
        .take_while(|(_, intersect_range)| intersect_range.start < range.end)
        .collect()
}

/// Inserts and merges ranges to ensure non overlapping and structured order in the cache
fn merge_ranges(
    col_cache: &mut CachedRanges,
    new_range: Range<u64>,
    new_data: ChecksummedBytes,
    col_range: &Range<u64>,
) -> Result<(), IntegrityError> {
    let mut merged_range = new_range.clone();
    let mut merged_data = new_data.clone();

    let mut ranges_to_remove = Vec::new();
    for (existing_range, existing_data) in col_cache.iter() {
        if merged_range.start <= existing_range.end && merged_range.end >= existing_range.start {
            let merged_start = merged_range.start.min(existing_range.start);
            let merged_end = merged_range.end.max(existing_range.end);

            let mut new_merged_data = ChecksummedBytes::default();
            if merged_start < merged_range.start {
                let prefix = existing_data.slice(0..(merged_range.start - existing_range.start) as usize);
                new_merged_data.extend(prefix)?;
            }

            let new_data_start = (merged_start.max(merged_range.start) - merged_range.start) as usize;
            let new_data_end = (merged_end.min(merged_range.end) - merged_range.start) as usize;
            new_merged_data.extend(merged_data.slice(new_data_start..new_data_end))?;

            if merged_end > merged_range.end {
                let suffix = existing_data.slice((merged_range.end - existing_range.start) as usize..);
                new_merged_data.extend(suffix)?;
            }

            merged_range = merged_start..merged_end;
            merged_data = new_merged_data;
            ranges_to_remove.push(existing_range.clone());
        }
    }

    // Clip the merged range to the column range in case it goes over
    let clipped_range = merged_range.start.max(col_range.start)..merged_range.end.min(col_range.end);
    let clipped_data = merged_data
        .slice((clipped_range.start - merged_range.start) as usize..(clipped_range.end - merged_range.start) as usize);

    for range in ranges_to_remove {
        col_cache.remove(&range);
    }

    col_cache.insert(RangeKey(clipped_range), clipped_data);
    Ok(())
}

/// Check if ranges overlap
fn intersect_ranges(a: &Range<u64>, b: &Range<u64>) -> Option<Range<u64>> {
    let start = a.start.max(b.start);
    let end = a.end.min(b.end);
    trace!("intersect_ranges: {:?} vs {:?}", a, b);
    if start < end {
        Some(start..end)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checksums::ChecksummedBytes;
    use crate::object::ObjectId;
    use crate::prefetch::lru_cache::{CacheKey, LruCache};
    use bytes::Bytes;
    use intervaltree::IntervalTree;
    use mountpoint_s3_client::types::ETag;
    use std::collections::BTreeMap;
    use std::iter::FromIterator;

    #[test]
    fn test_merge_ranges() {
        let mut col_cache = BTreeMap::new();
        let col_range = 0..100;

        // Test case 1: Insert non-overlapping range
        let new_range = 0..50;
        let new_data = ChecksummedBytes::new(Bytes::from_static(&[1; 50]));
        merge_ranges(&mut col_cache, new_range.clone(), new_data.clone(), &col_range).unwrap();
        assert_eq!(col_cache.len(), 1);
        assert_eq!(col_cache.get(&RangeKey(0..50)).unwrap().len(), 50);

        // Test case 2: Insert overlapping range
        let new_range = 25..75;
        let new_data = ChecksummedBytes::new(Bytes::from_static(&[2; 50]));
        merge_ranges(&mut col_cache, new_range.clone(), new_data.clone(), &col_range).unwrap();
        assert_eq!(col_cache.len(), 1);
        assert_eq!(col_cache.get(&RangeKey(0..75)).unwrap().len(), 75);

        // Test case 3: Insert range that extends beyond column range
        let new_range = 90..110;
        let new_data = ChecksummedBytes::new(Bytes::from_static(&[3; 20]));
        merge_ranges(&mut col_cache, new_range.clone(), new_data.clone(), &col_range).unwrap();
        assert_eq!(col_cache.len(), 2);
        assert_eq!(col_cache.get(&RangeKey(90..100)).unwrap().len(), 10);

        // Test case 4: Insert range that completely overlaps existing ranges
        let new_range = 0..100;
        let new_data = ChecksummedBytes::new(Bytes::from_static(&[4; 100]));
        merge_ranges(&mut col_cache, new_range.clone(), new_data.clone(), &col_range).unwrap();
        assert_eq!(col_cache.len(), 1);
        assert_eq!(col_cache.get(&RangeKey(0..100)).unwrap().len(), 100);
    }

    #[test]
    fn test_intersect_ranges() {
        assert_eq!(intersect_ranges(&(0..10), &(5..15)), Some(5..10));
        assert_eq!(intersect_ranges(&(0..10), &(10..20)), None);
        assert_eq!(intersect_ranges(&(0..10), &(5..8)), Some(5..8));
        assert_eq!(intersect_ranges(&(0..10), &(0..10)), Some(0..10));
        assert_eq!(intersect_ranges(&(0..10), &(11..20)), None);
        assert_eq!(intersect_ranges(&(5..15), &(0..10)), Some(5..10));
        assert_eq!(intersect_ranges(&(0..5), &(5..10)), None);
    }

    #[test]
    fn test_get_row_groups_and_columns() {
        let interval_tree: IntervalTree<u64, (usize, usize)> = IntervalTree::from_iter(vec![
            (0..100, (0, 0)),
            (100..200, (0, 1)),
            (200..300, (1, 0)),
            (300..400, (1, 1)),
        ]);

        let result = get_row_groups_and_columns(&interval_tree, 50..250);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ((0, 0), 50..100));
        assert_eq!(result[1], ((0, 1), 100..200));
        assert_eq!(result[2], ((1, 0), 200..250));

        let result = get_row_groups_and_columns(&interval_tree, 0..400);
        assert_eq!(result.len(), 4);

        let result = get_row_groups_and_columns(&interval_tree, 150..350);
        assert_eq!(result.len(), 3);

        let result = get_row_groups_and_columns(&interval_tree, 0..50);
        assert_eq!(result.len(), 1);

        let result = get_row_groups_and_columns(&interval_tree, 400..500);
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_move_entry_to_back() {
        let lru_cache = AsyncRwLock::new(LruCache::new(1000));
        let id = ObjectId::new("test".to_string(), ETag::for_tests());
        let row_group_col = (0, 0);

        // Add some entries
        {
            let mut cache = lru_cache.write().await;
            cache.add_entry(
                CacheKey {
                    file_id: id.clone(),
                    row_group: 0,
                    column: 0,
                },
                100,
            );
            cache.add_entry(
                CacheKey {
                    file_id: id.clone(),
                    row_group: 0,
                    column: 1,
                },
                100,
            );
            cache.add_entry(
                CacheKey {
                    file_id: id.clone(),
                    row_group: 1,
                    column: 0,
                },
                100,
            );
        }

        move_entry_to_back(&id, &row_group_col, &lru_cache).await;

        // Check if the entry was moved to the back
        {
            let cache = lru_cache.read().await;
            let entries: Vec<_> = cache.entries.keys().collect();
            assert_eq!(entries.last().unwrap().row_group, 0);
            assert_eq!(entries.last().unwrap().column, 0);
        }

        // Move a non-existent entry
        move_entry_to_back(&id, &(2, 0), &lru_cache).await;

        // Check if the cache order remains unchanged
        {
            let cache = lru_cache.read().await;
            let entries: Vec<_> = cache.entries.keys().collect();
            assert_eq!(entries.last().unwrap().row_group, 0);
            assert_eq!(entries.last().unwrap().column, 0);
        }
    }

    #[tokio::test]
    async fn test_lru_record() {
        let lru_cache = AsyncRwLock::new(LruCache::new(1000));
        let mut cache = InMemoryCache::new();
        let id = ObjectId::new("test".to_string(), ETag::for_tests());
        let row_group_col = (0, 0);
        let part = Part::new(id.clone(), 0, ChecksummedBytes::new(Bytes::from_static(&[1; 100])));

        lru_record(&id, row_group_col, &part, &lru_cache, &mut cache).await;

        // Check if the entry was added to the LRU cache
        {
            let cache = lru_cache.read().await;
            assert_eq!(cache.entries.len(), 1);
            assert_eq!(
                cache
                    .entries
                    .get(&CacheKey {
                        file_id: id.clone(),
                        row_group: 0,
                        column: 0
                    })
                    .unwrap(),
                &100
            );
        }

        // Add more entries to test eviction
        let part2 = Part::new(id.clone(), 0, ChecksummedBytes::new(Bytes::from_static(&[2; 901])));
        lru_record(&id, (0, 1), &part2, &lru_cache, &mut cache).await;

        // Check if the first entry was evicted
        {
            let cache = lru_cache.read().await;
            assert_eq!(cache.entries.len(), 1);
            assert_eq!(
                cache
                    .entries
                    .get(&CacheKey {
                        file_id: id.clone(),
                        row_group: 0,
                        column: 1
                    })
                    .unwrap(),
                &901
            );
        }

        // Test with a different file id
        let id2 = ObjectId::new("test2".to_string(), ETag::for_tests());
        let part3 = Part::new(id2.clone(), 0, ChecksummedBytes::new(Bytes::from_static(&[3; 50])));
        lru_record(&id2, (0, 0), &part3, &lru_cache, &mut cache).await;

        // Check if both entries are present (total size is less than 1000)
        {
            let cache = lru_cache.read().await;
            assert_eq!(cache.entries.len(), 2);
            assert_eq!(
                cache
                    .entries
                    .get(&CacheKey {
                        file_id: id.clone(),
                        row_group: 0,
                        column: 1
                    })
                    .unwrap(),
                &901
            );
            assert_eq!(
                cache
                    .entries
                    .get(&CacheKey {
                        file_id: id2.clone(),
                        row_group: 0,
                        column: 0
                    })
                    .unwrap(),
                &50
            );
        }
    }
}
