use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use async_trait::async_trait;
use futures::task::{Spawn, SpawnExt};
use futures::{pin_mut, StreamExt};
use intervaltree::IntervalTree;
use mountpoint_s3_client::{types::ETag, ObjectClient};
use tracing::{debug_span, error, trace, warn, Instrument};

use crate::checksums::{ChecksummedBytes, IntegrityError};
use crate::object::ObjectId;
use crate::prefetch::part::Part;
use crate::prefetch::part_queue::unbounded_part_queue;
use crate::prefetch::part_queue::PartQueueProducer;
use crate::prefetch::part_stream::{ObjectPartStream, RequestRange};
use crate::prefetch::task::RequestTask;
use crate::prefetch::InMemoryCache;
use crate::prefetch::{PrefetchReadError, RawMetadata};

use super::parquet_prefetch::RangeKey;
use super::{MetadataRef, RawMetadataRef};

type RowGroupIndex = usize;
type ColumnIndex = usize;

#[derive(Debug)]
pub struct ParquetPartStream<Runtime> {
    runtime: Runtime,
}

impl<Runtime> ParquetPartStream<Runtime> {
    pub fn new(runtime: Runtime) -> Self {
        Self { runtime }
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
        in_mem_cache: InMemoryCache,
        parsed_metadata: MetadataRef,
        raw_metadata: RawMetadataRef,
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
            let id = ObjectId::new(key.to_owned(), if_match);
            let span = debug_span!("prefetch", range=?range);

            async move {
                let metadata = parsed_metadata.read().await.clone();
                let request_range = range.start()..range.end();

                let mut remaining_range = request_range.clone();
                let mut metadata_range = None;
                let mut metadata_present = false;

                if let Some(raw_metadata_range) = get_raw_metadata_range(&raw_metadata).await {
                    if let Some(intersection) = intersect_ranges(&remaining_range, &raw_metadata_range) {
                        trace!("Metadata portion detected: fetching from cache");
                        metadata_range = Some(intersection.clone());
                        metadata_present = true;
                        remaining_range = remaining_range.start..intersection.start;
                    }
                }

                let mut rowgroup_cols: Vec<((RowGroupIndex, ColumnIndex), Range<u64>)> =
                    if let Some(metadata) = &metadata {
                        get_row_groups_and_columns(metadata, remaining_range.clone())
                    } else {
                        Vec::new()
                    };

                while !remaining_range.is_empty() {
                    if !try_serve_from_cache(
                        &mut remaining_range,
                        &metadata,
                        &in_mem_cache,
                        &id,
                        &part_queue_producer,
                        &rowgroup_cols,
                    )
                    .await
                    {
                        if let Err(e) = fetch_from_client(
                            &client,
                            &bucket,
                            &id,
                            &mut remaining_range,
                            preferred_part_size,
                            &in_mem_cache,
                            &mut rowgroup_cols,
                            &part_queue_producer,
                        )
                        .await
                        {
                            error!("Error fetching from client: {:?}", e);
                            part_queue_producer.push(Err(e));
                            return;
                        }
                    }
                }

                if metadata_present {
                    serve_metadata(&raw_metadata, metadata_range, &id, &part_queue_producer).await;
                }

                trace!("request finished");
            }
            .instrument(span)
        };

        let task_handle = self.runtime.spawn_with_handle(request_task).unwrap();
        RequestTask::from_handle(task_handle, size, start, part_queue)
    }
}

/// Returns the raw metadata range if available
async fn get_raw_metadata_range(raw_metadata: &RawMetadataRef) -> Option<Range<u64>> {
    raw_metadata.read().await.as_ref().map(|m| m.range.clone())
}

/// Tries to serve the requested range from the cache
async fn try_serve_from_cache<E: std::error::Error + Send + Sync + 'static>(
    remaining_range: &mut Range<u64>,
    metadata: &Option<IntervalTree<u64, (RowGroupIndex, ColumnIndex)>>,
    in_mem_cache: &InMemoryCache,
    id: &ObjectId,
    part_queue_producer: &crate::prefetch::part_queue::PartQueueProducer<E>,
    cols: &Vec<((RowGroupIndex, ColumnIndex), Range<u64>)>,
) -> bool {
    if metadata.is_some() {
        let mut rowgroup_col_cache = in_mem_cache.write().await;

        for (row_group_col, col_range) in cols {
            if let Some(col_cache) = rowgroup_col_cache
                .as_mut()
                .and_then(|cache| cache.get_mut(row_group_col))
            {
                let cached_ranges: Vec<_> = col_cache.keys().cloned().collect();
                trace!(
                    "For row group col {:?}, Cached ranges: {:?}",
                    row_group_col,
                    cached_ranges
                );

                for cached_range in &cached_ranges {
                    if cached_range.range.start >= col_range.end {
                        break;
                    }

                    if let Some(intersection) = intersect_ranges(&cached_range.range, remaining_range) {
                        let data = col_cache.get(cached_range).unwrap();
                        let part_start = intersection.start;
                        let part_end = intersection.end;
                        let data_offset = part_start - cached_range.range.start;

                        if part_start == remaining_range.start {
                            let part_data =
                                data.slice(data_offset as usize..(data_offset + (part_end - part_start)) as usize);
                            let part = Part::new(id.clone(), part_start, part_data);

                            trace!("Pushing part to queue from cache: {:?}", part_start..part_end);
                            part_queue_producer.push(Ok(part));

                            *remaining_range = part_end..remaining_range.end;
                            if remaining_range.is_empty() {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

/// Fetches the requested range from the client
#[allow(clippy::too_many_arguments)]
async fn fetch_from_client<Client>(
    client: &Client,
    bucket: &str,
    id: &ObjectId,
    remaining_range: &mut Range<u64>,
    preferred_part_size: usize,
    in_mem_cache: &InMemoryCache,
    cols: &mut Vec<((RowGroupIndex, ColumnIndex), Range<u64>)>,
    part_queue_producer: &PartQueueProducer<Client::ClientError>,
) -> Result<(), PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    trace!("Fetching from client for range: {:?}", remaining_range);
    let prefetch_range = RequestRange::new(
        remaining_range.end as usize,
        remaining_range.start,
        cmp::min(preferred_part_size as u64, remaining_range.end - remaining_range.start) as usize,
    );

    match get_from_client(client, bucket, id, prefetch_range).await {
        Ok(parts) => {
            let mut rowgroup_col_cache = in_mem_cache.write().await;

            for part in parts {
                let part_range = part.offset()..part.offset() + part.len() as u64;
                trace!("Pushing part to queue from client: {:?}", part_range);
                part_queue_producer.push(Ok(part.clone()));

                if rowgroup_col_cache.is_none() {
                    *rowgroup_col_cache = Some(HashMap::new());
                }

                let split_index: usize = cols.partition_point(|(_, col_range)| col_range.start < part_range.end);
                cols.truncate(split_index);

                for ((row_group, column), _) in cols.iter() {
                    let col_cache = rowgroup_col_cache
                        .as_mut()
                        .unwrap()
                        .entry((*row_group, *column))
                        .or_insert_with(BTreeMap::new);

                    if let Err(e) = merge_ranges(col_cache, part_range.clone(), part.get_check_summed_bytes().clone()) {
                        error!("Error merging ranges: {:?}", e);
                    }
                }

                *remaining_range = part_range.end..remaining_range.end;
                if remaining_range.is_empty() {
                    break;
                }
            }
        }
        Err(e) => {
            error!(key=id.key(), error=?e, "GetObject request failed");
            return Err(e);
        }
    }

    Ok(())
}

/// Serves the metadata range from the raw metadata if required
async fn serve_metadata<E: std::error::Error + Send + Sync + 'static>(
    raw_metadata: &RawMetadataRef,
    metadata_range: Option<Range<u64>>,
    id: &ObjectId,
    part_queue_producer: &crate::prefetch::part_queue::PartQueueProducer<E>,
) {
    if let Some(metadata_range) = metadata_range {
        trace!("Serving metadata range: {:?}", &metadata_range);
        let raw_metadata_lock = raw_metadata.read().await;
        if let Some(RawMetadata {
            bytes: raw_metadata,
            range: metadata_start_end,
        }) = &*raw_metadata_lock
        {
            let metadata_start = (metadata_range.start - metadata_start_end.start) as usize;
            let metadata_end = (metadata_range.end - metadata_start_end.start) as usize;
            let metadata_bytes = raw_metadata.slice(metadata_start..metadata_end);
            trace!("Metadata range start-end: {:?}-{:?}", metadata_start, metadata_end);
            let metadata_part = Part::new(id.clone(), metadata_range.start, metadata_bytes);
            part_queue_producer.push(Ok(metadata_part));
        }
    }
}

/// Get from client as requested
async fn get_from_client<Client>(
    client: &Client,
    bucket: &str,
    id: &ObjectId,
    range: RequestRange,
) -> Result<Vec<Part>, PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    let key = id.key();
    let block_aligned_byte_range = range;

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
    let mut offset = range.start();

    while let Some(result) = get_object_result.next().await {
        match result {
            Ok((part_offset, body)) => {
                trace!(offset, length = body.len(), "received GetObject part");
                metrics::counter!("s3.client.total_bytes", "type" => "read").increment(body.len() as u64);

                if part_offset != offset {
                    warn!(key, part_offset, offset, "wrong offset for GetObject body part");
                    return Err(PrefetchReadError::GetRequestReturnedWrongOffset {
                        offset: part_offset,
                        expected_offset: offset,
                    });
                }

                let body_len = body.len();
                let part = Part::new(id.clone(), offset, ChecksummedBytes::new(body.into()));
                parts.push(part);
                offset += body_len as u64;
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
fn get_row_groups_and_columns(
    interval_tree: &IntervalTree<u64, (RowGroupIndex, ColumnIndex)>,
    range: Range<u64>,
) -> Vec<((RowGroupIndex, ColumnIndex), Range<u64>)> {
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

/// Merge a new range with the existing cache
fn merge_ranges(
    col_cache: &mut BTreeMap<RangeKey, ChecksummedBytes>,
    new_range: Range<u64>,
    new_data: ChecksummedBytes,
) -> Result<(), IntegrityError> {
    let mut ranges_to_remove = Vec::new();
    let mut merged_range = new_range.clone();
    let mut merged_data = new_data;

    for (existing_range, existing_data) in col_cache.iter() {
        if merged_range.start <= existing_range.range.end && merged_range.end >= existing_range.range.start {
            let merged_start = merged_range.start.min(existing_range.range.start);
            let merged_end = merged_range.end.max(existing_range.range.end);

            let mut new_merged_data = ChecksummedBytes::default();

            if merged_start < merged_range.start {
                let prefix = existing_data.slice(0..(merged_range.start - existing_range.range.start) as usize);
                new_merged_data.extend(prefix)?;
            }

            let new_data_start = (merged_start.max(merged_range.start) - merged_range.start) as usize;
            let new_data_end = (merged_end.min(merged_range.end) - merged_range.start) as usize;
            new_merged_data.extend(merged_data.slice(new_data_start..new_data_end))?;

            if merged_end > merged_range.end {
                let suffix = existing_data.slice((merged_range.end - existing_range.range.start) as usize..);
                new_merged_data.extend(suffix)?;
            }

            merged_range = merged_start..merged_end;
            merged_data = new_merged_data;
            ranges_to_remove.push(existing_range.clone());
        }
    }

    for range in &ranges_to_remove {
        col_cache.remove(range);
    }

    col_cache.insert(RangeKey { range: merged_range }, merged_data);

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
