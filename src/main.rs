use std::{collections::BTreeMap, net::SocketAddr};

use bucket_config::MergedBucket;
use hyper::Server;
use log::debug;
use s3s::{
    dto::{
        DeleteObjectInput, DeleteObjectOutput, GetObjectInput, GetObjectOutput, ListObjectsInput,
        ListObjectsOutput, Object,
    },
    service::S3Service,
    S3Error, S3Result, S3,
};
use s3s_aws::conv::{try_from_aws, try_into_aws};

mod bucket_config;
mod rfc2822_timestamp;
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
    async fn get_object(&self, _input: GetObjectInput) -> S3Result<GetObjectOutput> {
        let bucket = self.buckets.get(&_input.bucket);
        if bucket.is_none() {
            return Err(S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let aws_input = try_into_aws(_input).expect("Failed to convert GetObjectInput to AWS");

        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.get_object(&aws_input));

        match utils::select_ok(futures).await {
            Ok(output) => Ok(try_from_aws(output).expect("Failed to parse output")),
            Err(_) => Err(S3Error::new(s3s::S3ErrorCode::NoSuchKey)),
        }
    }

    async fn delete_object(&self, _input: DeleteObjectInput) -> S3Result<DeleteObjectOutput> {
        let bucket = self.buckets.get(&_input.bucket);
        if bucket.is_none() {
            return Err(S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        }
        let bucket = bucket.unwrap();

        let aws_input = try_into_aws(_input).expect("Failed to convert DeleteObjectInput to AWS");

        let futures = bucket
            .as_content()
            .into_iter()
            .map(|source_bucket| source_bucket.delete_object(&aws_input));

        // run all delete operations in parallel
        // if any of them succeeds, we return success
        for res in futures::future::join_all(futures).await {
            if let Ok(output) = res {
                return Ok(try_from_aws(output).expect("Failed to parse output"));
            }
        }
        Err(S3Error::new(s3s::S3ErrorCode::NoSuchKey))
    }

    async fn list_objects(&self, _input: ListObjectsInput) -> S3Result<ListObjectsOutput> {
        // list objects from all buckets starting at marker, merge the results until we exhaust
        // the list or get to the limit
        let max_keys = if _input.max_keys == 0 {
            1000
        } else {
            i32::min(1000, _input.max_keys)
        };

        debug!("max_keys: {:?}", max_keys);
        Ok(ListObjectsOutput {
            contents: Some(vec![Object {
                key: Some("test.txt".to_string()),
                size: 13989,
                ..Default::default()
            }]),
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    let service = S3Service::new(Box::new(MergerS3::new().await))
        .into_shared()
        .into_make_service();
    let server = Server::bind(&addr).serve(service);

    println!("Listening on http://{}", addr);
    server.await?;
    Ok(())
}
