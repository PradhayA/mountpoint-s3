use crate::sync::Arc;
use async_trait::async_trait;
use bytes::Bytes;
use futures::pin_mut;
use futures::StreamExt;
use mountpoint_s3_client::types::ETag;
use mountpoint_s3_client::ObjectClient;
use parquet::errors::ParquetError;
use parquet::file::footer::decode_footer;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use tracing::debug_span;
use tracing::trace;

use super::PrefetchReadError;

pub async fn read_parquet_metadata<Client>(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
    if_match: ETag,
    total_size: u64,
) -> Result<Bytes, PrefetchReadError<Client::ClientError>>
where
    Client: ObjectClient + Send + Sync + 'static,
{
    let chunk_reader = ParquetS3ChunkReader {
        client,
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        etag: if_match,
        size: total_size,
    };

    parse_metadata_raw(&chunk_reader).map_err(|_| PrefetchReadError::GetRequestTerminatedUnexpectedly)
}

pub fn parse_byte_ranges(metadata: &ParquetMetaData) -> HashMap<(usize, usize), (u64, u64)> {
    let mut byte_ranges = HashMap::new();

    for (rowgroup_index, rowgroup) in metadata.row_groups().iter().enumerate() {
        for (column_index, column_metadata) in rowgroup.columns().iter().enumerate() {
            let start_byte = column_metadata.file_offset() as u64;
            let end_byte = start_byte + column_metadata.compressed_size() as u64 - 1;

            byte_ranges.insert((rowgroup_index, column_index), (start_byte, end_byte));
        }
    }

    byte_ranges
}

fn parse_metadata_raw<R: ChunkReader>(chunk_reader: &R) -> Result<Bytes, ParquetError> {
    let footer_size = 8;
    // check file is large enough to hold footer
    let file_size = chunk_reader.len();
    if file_size < (footer_size as u64) {
        return Err(ParquetError::General(
            "Invalid Parquet file. Size is smaller than footer".to_string(),
        ));
    }

    let mut footer = [0_u8; 8];
    chunk_reader.get_read(file_size - 8, 8)?.read_exact(&mut footer)?;

    let metadata_len = decode_footer(&footer)?;
    let footer_metadata_len = footer_size + metadata_len;

    if footer_metadata_len > file_size as usize {
        return Err(ParquetError::General(
            "Invalid Parquet file. Reported metadata length is shorter than expected".to_string(),
        ));
    }

    let metadata = chunk_reader.get_bytes(file_size - footer_metadata_len as u64, metadata_len)?;

    return Ok(metadata);
}

struct ParquetS3ChunkReader<Client: ObjectClient> {
    client: Arc<Client>,
    bucket: String,
    key: String,
    etag: ETag,
    size: u64,
}

impl<Client: ObjectClient + Send + Sync + 'static> ChunkReader for ParquetS3ChunkReader<Client> {
    type T = S3ReadSeek<Client>;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T, ParquetError> {
        Ok(S3ReadSeek {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            etag: self.etag.clone(),
            position: start,
            size: self.size,
            length,
        })
    }
}

struct S3ReadSeek<Client: ObjectClient> {
    client: Arc<Client>,
    bucket: String,
    key: String,
    etag: ETag,
    position: u64,
    size: u64,
    length: usize,
}

impl<Client: ObjectClient + Send + Sync + 'static> Read for S3ReadSeek<Client> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        trace!("Entered here for reader s3seek??");

        let range = self.position..(self.position + self.length.min(buf.len()) as u64).min(self.size);
        if range.is_empty() {
            return Ok(0);
        }

        let span = debug_span!("read", range = ?range);
        let _enter = span.enter();

        let expected_size = range.end - range.start;
        let mut data = Vec::with_capacity(expected_size as usize);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let read_len = runtime.block_on(async {
            let get_object_result = self
                .client
                .get_object(&self.bucket, &self.key, Some(range), Some(self.etag.clone()))
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            pin_mut!(get_object_result);

            while let Some(result) = get_object_result.next().await {
                match result {
                    Ok((_, body)) => {
                        trace!(length = body.len(), "received part");
                        metrics::counter!("s3.client.total_bytes", "type" => "read").increment(body.len() as u64);
                        data.extend_from_slice(&body);
                    }
                    Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
                }
            }

            trace!("data fetched");
            Ok(data.len())
        })?;

        buf[..read_len].copy_from_slice(&data);
        self.position += read_len as u64;
        self.length -= read_len;
        Ok(read_len)
    }
}

impl<Client: ObjectClient> Seek for S3ReadSeek<Client> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => (self.size as i64 + offset) as u64,
            SeekFrom::Current(offset) => (self.position as i64 + offset) as u64,
        };
        Ok(self.position)
    }
}

impl<Client: ObjectClient> Length for ParquetS3ChunkReader<Client> {
    fn len(&self) -> u64 {
        self.size
    }
}
