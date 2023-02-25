use std::{collections::BTreeMap, net::SocketAddr, time::SystemTime};

use bucket_config::MergedBucket;
use hyper::Server;
use log::debug;
use s3s::{
    dto::{
        Bucket, CommonPrefixList, CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource,
        DeleteObjectInput, DeleteObjectOutput, GetObjectInput, GetObjectOutput, HeadObjectInput,
        HeadObjectOutput, ListBucketsInput, ListBucketsOutput, ListObjectsInput, ListObjectsOutput,
        NextMarker, ObjectList, PutObjectInput, PutObjectOutput, Timestamp,
    },
    s3_error,
    service::S3Service,
    stream::ByteStream,
    S3Auth, S3Error, S3Result, S3,
};
use s3s_aws::conv::{try_from_aws, try_into_aws};

mod bucket_config;
mod utils;

struct MergerS3 {
    buckets: BTreeMap<String, MergedBucket>,
}

impl MergerS3 {
    pub async fn new() -> Self {
        Self {
            buckets: bucket_config::read_config().await,
        }
    }
}

#[async_trait::async_trait]
impl S3 for MergerS3 {
    async fn list_buckets(&self, input: ListBucketsInput) -> S3Result<ListBucketsOutput> {
        debug!("list_buckets({:#?})", input);
        let buckets: Vec<Bucket> = self
            .buckets
            .keys()
            .cloned()
            .map(|name| Bucket {
                name: Some(name),
                creation_date: Some(Timestamp::from(SystemTime::now())),
            })
            .collect();

        Ok(ListBucketsOutput {
            buckets: if buckets.is_empty() {
                None
            } else {
                Some(buckets)
            },
            owner: Some(s3s::dto::Owner {
                display_name: Some("mergers3".to_string()),
                id: Some("mergers3".to_string()),
            }),
        })
    }

    async fn head_object(&self, input: HeadObjectInput) -> S3Result<HeadObjectOutput> {
        debug!("head_object({:#?})", input);
        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(s3_error!(NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let aws_input = try_into_aws(input).expect("Failed to convert GetObjectInput to AWS");

        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.head_object(&aws_input));

        match utils::select_ok(futures).await {
            Ok(output) => Ok(try_from_aws(output).expect("Failed to parse output")),
            Err(_) => Err(s3_error!(NoSuchKey)),
        }
    }

    async fn get_object(&self, input: GetObjectInput) -> S3Result<GetObjectOutput> {
        debug!("get_object({:#?})", input);
        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(s3_error!(NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let aws_input = try_into_aws(input).expect("Failed to convert GetObjectInput to AWS");

        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.get_object(&aws_input));

        match utils::select_ok(futures).await {
            Ok(output) => Ok(try_from_aws(output).expect("Failed to parse output")),
            Err(_) => Err(s3_error!(NoSuchKey)),
        }
    }

    async fn delete_object(&self, input: DeleteObjectInput) -> S3Result<DeleteObjectOutput> {
        debug!("delete_object({:#?})", input);
        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(s3_error!(NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let aws_input = try_into_aws(input).expect("Failed to convert DeleteObjectInput to AWS");

        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.delete_object_and_update_size(&aws_input));

        // run all delete operations in parallel
        // if any of them succeeds, we return success
        let mut ret: Option<DeleteObjectOutput> = None;
        for res in futures::future::join_all(futures).await {
            if let Ok(output) = res {
                if ret.is_none() {
                    ret = Some(try_from_aws(output).expect("Failed to parse output"));
                }
            }
        }
        match ret {
            Some(output) => Ok(output),
            None => Err(s3_error!(NoSuchKey)),
        }
    }

    async fn list_objects(&self, input: ListObjectsInput) -> S3Result<ListObjectsOutput> {
        debug!("list_objects({:#?})", input);
        // list objects from all buckets starting at marker, merge the results until we exhaust
        // the list or get to the limit
        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(s3_error!(NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let name = input.bucket.clone();
        let prefix = input.prefix.clone();
        let delimiter = input.delimiter.clone();
        let marker = input.marker.clone();
        let max_keys = input.max_keys;

        let aws_input = try_into_aws(input).expect("Failed to convert ListObjectsInput to AWS");
        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.list_objects(&aws_input));

        // TODO: temporary hack: just join all commonprefixes and objects,
        // return the lexically smallest next marker
        let mut common_prefixes: CommonPrefixList = Vec::new();
        let mut objects: ObjectList = Vec::new();
        let mut is_truncated = false;
        let mut next_marker: Option<NextMarker> = None;

        for res in futures::future::join_all(futures).await {
            if let Ok(output) = res {
                if let Some(src_prefixes) = output.common_prefixes() {
                    common_prefixes.extend(
                        src_prefixes
                            .iter()
                            .map(|p| try_from_aws(p.clone()).unwrap()),
                    );
                }
                if let Some(src_objects) = output.contents() {
                    objects.extend(src_objects.iter().map(|o| try_from_aws(o.clone()).unwrap()));
                }

                is_truncated |= output.is_truncated();
                if let Some(src_next_marker) = output.next_marker() {
                    let src_next_marker = src_next_marker.to_string();
                    if let Some(next_marker) = &mut next_marker {
                        if src_next_marker < *next_marker {
                            *next_marker = src_next_marker;
                        }
                    } else {
                        next_marker = Some(src_next_marker);
                    }
                }
            }
        }

        common_prefixes.sort_by(|a, b| a.prefix.cmp(&b.prefix));
        objects.sort_by(|a, b| a.key.cmp(&b.key));

        Ok(ListObjectsOutput {
            common_prefixes: if common_prefixes.len() > 0 {
                Some(common_prefixes)
            } else {
                None
            },
            contents: if objects.len() > 0 {
                Some(objects)
            } else {
                None
            },
            is_truncated,
            next_marker,
            delimiter,
            marker,
            max_keys,
            prefix,
            name: Some(name),
            ..Default::default()
        })
    }

    async fn put_object(&self, input: PutObjectInput) -> S3Result<PutObjectOutput> {
        debug!("put_object({:#?})", input);
        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let body_len: u64 = (&input.body)
            .as_ref()
            .map(|b| {
                b.remaining_length()
                    .exact()
                    .expect("Failed to get remaining length")
            })
            .unwrap_or(0)
            .try_into()
            .unwrap();
        let aws_input = try_into_aws(input).expect("Failed to convert GetObjectInput to AWS");

        let source_bucket = bucket.get_source_bucket(body_len).await;
        if source_bucket.is_none() {
            return Err(s3_error!(InvalidBucketState));
        }
        let source_bucket = source_bucket.unwrap();

        debug!("PUT object bytes {}", body_len);
        debug!("Chose source bucket {}", source_bucket.get_name());
        match source_bucket
            .put_object_and_update_size(aws_input, body_len)
            .await
        {
            Ok(output) => Ok(try_from_aws(output).expect("Failed to parse output")),
            Err(_) => Err(s3_error!(InternalError)),
        }
    }

    async fn copy_object(&self, input: CopyObjectInput) -> S3Result<CopyObjectOutput> {
        debug!("copy_object({:#?})", input);
        // HEAD object to see which bucket it's in
        // if we have enough space in the source bucket, copy it
        // otherwise, copy it to another available bucket

        // we only support copying within the same bucket
        let (copy_source_bucket, copy_source_key) = match input.copy_source {
            CopySource::Bucket {
                ref bucket,
                ref key,
                version_id: _,
            } => (bucket.to_string(), key.to_string()),
            _ => return Err(s3_error!(InvalidRequest)),
        };

        // TODO: support copying from other buckets
        // TODO: copy object metadata if requested
        if input.bucket != copy_source_bucket {
            return Err(s3_error!(InvalidRequest));
        }

        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(s3_error!(NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let source = bucket.find_object_source(&copy_source_key).await;
        if source.is_none() {
            return Err(s3_error!(NoSuchKey));
        }
        let (source_bucket, head_output) = source.unwrap();

        let object_size: u64 = head_output.content_length().try_into().unwrap();
        if source_bucket.has_bytes(object_size).await {
            debug!("Source bucket has enough space, copying object using CopyObject");
            let mut aws_input =
                try_into_aws(input).expect("Failed to convert CopyObjectInput to AWS");
            aws_input.copy_source = Some(
                CopySource::Bucket {
                    bucket: source_bucket.get_name().into(),
                    key: copy_source_key.into_boxed_str(),
                    version_id: None,
                }
                .format_to_string(),
            );
            if let Ok(output) = source_bucket
                .copy_object_and_update_size(&aws_input, object_size)
                .await
            {
                Ok(try_from_aws(output).expect("Failed to parse output"))
            } else {
                Err(s3_error!(InternalError))
            }
        } else {
            debug!("Source bucket doesn't have enough space, copying object using GetObject + PutObject");
            let get_object_input = aws_sdk_s3::input::GetObjectInput::builder()
                .key(copy_source_key)
                .build()
                .expect("Failed to build GetObjectInput");

            let get_output = source_bucket
                .get_object(&get_object_input)
                .await
                .expect("Failed to get object");
            let last_modified: Option<Timestamp> =
                try_from_aws(get_output.last_modified().copied()).unwrap();

            let mut put_object_input = aws_sdk_s3::input::PutObjectInput::builder().key(input.key);
            put_object_input = put_object_input
                .set_cache_control(get_output.cache_control().map(|s| s.to_string()));
            put_object_input = put_object_input
                .set_content_disposition(get_output.content_disposition().map(|s| s.to_string()));
            put_object_input = put_object_input
                .set_content_encoding(get_output.content_encoding().map(|s| s.to_string()));
            put_object_input = put_object_input
                .set_content_language(get_output.content_language().map(|s| s.to_string()));
            put_object_input =
                put_object_input.set_content_type(get_output.content_type().map(|s| s.to_string()));
            put_object_input = put_object_input.set_expires(get_output.expires().copied());
            put_object_input = put_object_input.set_body(Some(get_output.body));
            let put_object_input = put_object_input
                .build()
                .expect("Failed to build PutObjectInput");

            match source_bucket
                .put_object_and_update_size(put_object_input, object_size)
                .await
            {
                Ok(output) => {
                    return Ok(CopyObjectOutput {
                        copy_object_result: Some(CopyObjectResult {
                            e_tag: output.e_tag().map(|s| s.to_string()),
                            last_modified,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                }
                Err(_) => Err(s3_error!(InternalError)),
            }
        }
    }
}

struct MergerS3Auth {}

// must use authorized connections for CopyObject to work
#[async_trait::async_trait]
impl S3Auth for MergerS3Auth {
    async fn get_secret_key(&self, _access_key: &str) -> S3Result<String> {
        Ok("secret".to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let mut service = S3Service::new(Box::new(MergerS3::new().await));
    service.set_auth(Box::new(MergerS3Auth {}));
    let make_service = service.into_shared().into_make_service();

    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);
    server.await?;
    Ok(())
}
