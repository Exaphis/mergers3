use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    net::SocketAddr,
    time::SystemTime,
};

use bucket_config::{MergedBucket, SourceBucketDeleteObjectError};
use hyper::Server;
use log::{debug, warn};
use s3s::{
    dto::{
        Bucket, CommonPrefix, CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource,
        DeleteObjectInput, DeleteObjectOutput, EncodingType, GetObjectInput, GetObjectOutput,
        HeadObjectInput, HeadObjectOutput, ListBucketsInput, ListBucketsOutput, ListObjectsInput,
        ListObjectsOutput, NextMarker, PutObjectInput, PutObjectOutput, Timestamp,
    },
    s3_error,
    service::S3Service,
    stream::ByteStream,
    S3Auth, S3Error, S3Result, S3,
};
use s3s_aws::conv::{try_from_aws, try_into_aws};

mod bucket_config;
mod utils;

use crate::{bucket_config::SourceBucket, utils::ComparableObject};

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
        match bucket
            .first_found(&aws_input, |a, b| Box::pin(SourceBucket::head_object(a, b)))
            .await
        {
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

        match bucket
            .first_found(&aws_input, |a, b| Box::pin(SourceBucket::get_object(a, b)))
            .await
        {
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

        let (res, errs) = bucket
            .all(&aws_input, |a, b| {
                Box::pin(SourceBucket::delete_object_and_update_size(a, b))
            })
            .await;

        if res.is_empty() {
            for err in errs {
                if let SourceBucketDeleteObjectError::DeleteObject(err) = err {
                    warn!("delete_object received a delete object error: {:?}", err);
                }
            }
            Err(s3_error!(NoSuchKey))
        } else {
            Ok(try_from_aws(res[0].clone()).expect("Failed to parse output"))
        }
    }

    async fn list_objects(&self, input: ListObjectsInput) -> S3Result<ListObjectsOutput> {
        debug!("list_objects({:#?})", input);
        // list objects from all buckets starting at marker, merge the results until we exhaust
        // the list or get to the limit
        // list with no delimiter in order to get the next marker easily
        // if we hit a next marker, we must end the list and return it as the next marker
        if input.delimiter.is_some() && input.delimiter.as_ref().unwrap().chars().count() > 1 {
            return Err(s3_error!(InvalidArgument));
        }
        let delimiter = input.delimiter.as_ref().map(|d| d.chars().next().unwrap());

        let bucket = self.buckets.get(&input.bucket);
        if bucket.is_none() {
            return Err(s3_error!(NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let name = input.bucket.clone();
        let prefix = input.prefix.clone();
        let input_delimiter = input.delimiter.clone();
        let marker = input.marker.clone();
        let max_keys = if input.max_keys <= 0 {
            1000
        } else {
            input.max_keys.min(1000)
        };
        let encoding_type = input.encoding_type.clone();

        let mut aws_input = try_into_aws(input).expect("Failed to convert ListObjectsInput to AWS");
        aws_input.delimiter = None;
        aws_input.encoding_type = None;
        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.list_objects(&aws_input));

        let mut object_heap: BinaryHeap<Reverse<ComparableObject>> = BinaryHeap::new();
        for res in futures::future::join_all(futures).await {
            if let Ok(output) = res {
                object_heap.extend(
                    output
                        .contents
                        .into_iter()
                        .map(|objs| {
                            objs.into_iter().map(|obj| {
                                Reverse(ComparableObject(
                                    try_from_aws(obj).expect("Failed to parse Object"),
                                ))
                            })
                        })
                        .flatten(),
                );
            }
        }

        // use btreeset as there can be duplicate objects in different buckets
        let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut objects: BTreeSet<ComparableObject> = BTreeSet::new();
        let mut is_truncated = false;
        let mut next_marker: Option<NextMarker> = None;

        let prefix_len = prefix
            .as_ref()
            .map(|prefix| prefix.chars().count())
            .unwrap_or(0);

        while let Some(Reverse(ComparableObject(obj))) = object_heap.pop() {
            let key = obj.key.as_ref().unwrap();
            if objects.len() > max_keys as usize {
                is_truncated = true;
                next_marker = Some(key.clone());
                break;
            }

            if let Some(delim_pos) = key
                .chars()
                .skip(prefix_len)
                .position(|c| Some(c) == delimiter)
            {
                common_prefixes.insert(key.chars().take(prefix_len + delim_pos + 1).collect());
            } else {
                objects.insert(ComparableObject(obj));
            }
        }

        Ok(ListObjectsOutput {
            common_prefixes: if common_prefixes.len() > 0 {
                Some(
                    common_prefixes
                        .into_iter()
                        .map(|p| CommonPrefix { prefix: Some(p) })
                        .collect(),
                )
            } else {
                None
            },
            contents: if objects.len() > 0 {
                Some(
                    objects
                        .into_iter()
                        .map(|o| {
                            let mut o = o.0;
                            if encoding_type == Some(EncodingType::from_static(EncodingType::URL)) {
                                o.key = o.key.map(|k| {
                                    url::form_urlencoded::byte_serialize(k.as_bytes()).collect()
                                });
                            }
                            o
                        })
                        .collect(),
                )
            } else {
                None
            },
            is_truncated,
            next_marker,
            delimiter: input_delimiter,
            marker,
            max_keys,
            prefix,
            name: Some(name),
            encoding_type,
            ..Default::default()
        })
    }

    async fn put_object(&self, input: PutObjectInput) -> S3Result<PutObjectOutput> {
        debug!("put_object({:#?})", input);
        debug!("object key: {}", input.key);
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
            Err(e) => {
                warn!("Failed to put object: {}", e);
                Err(s3_error!(InternalError))
            }
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
