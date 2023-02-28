use std::{collections::BTreeMap, fmt::Debug, fs::File};

use aws_sdk_s3::{
    error::{
        CopyObjectError, DeleteObjectError, GetObjectError, HeadObjectError, ListObjectsError,
        PutObjectError,
    },
    input::{
        CopyObjectInput, DeleteObjectInput, GetObjectInput, HeadObjectInput, ListObjectsInput,
        PutObjectInput,
    },
    output::HeadObjectOutput,
    types::{ByteStream, SdkError},
    Client, Credentials, Region,
};
use futures::{future::BoxFuture, FutureExt};
use log::{debug, warn};
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer,
};
use tokio::{io::AsyncReadExt, sync::Mutex};

use crate::utils::{select_ok, select_some};

pub struct DeserializableBucket {
    client: Client,
    bucket_name: String,
}

// https://serde.rs/impl-deserialize.html
impl<'de> Deserialize<'de> for DeserializableBucket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            BucketName,
            Endpoint,
            AccessKey,
            SecretKey,
        }

        struct BucketVisitor;
        impl<'de> Visitor<'de> for BucketVisitor {
            type Value = DeserializableBucket;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct DeserializableBucket")
            }

            fn visit_map<V>(self, mut map: V) -> Result<DeserializableBucket, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut bucket_name: Option<String> = None;
                let mut endpoint: Option<String> = None;
                let mut access_key: Option<String> = None;
                let mut secret_key: Option<String> = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::BucketName => {
                            if bucket_name.is_some() {
                                return Err(serde::de::Error::duplicate_field("bucket_name"));
                            }
                            bucket_name = Some(map.next_value()?);
                        }
                        Field::Endpoint => {
                            if endpoint.is_some() {
                                return Err(serde::de::Error::duplicate_field("endpoint"));
                            }
                            endpoint = Some(map.next_value()?);
                        }
                        Field::AccessKey => {
                            if access_key.is_some() {
                                return Err(serde::de::Error::duplicate_field("access_key"));
                            }
                            access_key = Some(map.next_value()?);
                        }
                        Field::SecretKey => {
                            if secret_key.is_some() {
                                return Err(serde::de::Error::duplicate_field("secret_key"));
                            }
                            secret_key = Some(map.next_value()?);
                        }
                    }
                }

                let bucket_name =
                    bucket_name.ok_or_else(|| serde::de::Error::missing_field("bucket_name"))?;
                let endpoint =
                    endpoint.ok_or_else(|| serde::de::Error::missing_field("endpoint"))?;
                let access_key =
                    access_key.ok_or_else(|| serde::de::Error::missing_field("access_key"))?;
                let secret_key =
                    secret_key.ok_or_else(|| serde::de::Error::missing_field("secret_key"))?;

                let config = aws_sdk_s3::config::Builder::new()
                    .endpoint_url(endpoint)
                    .region(Region::new("us-east-1"))
                    .credentials_provider(Credentials::from_keys(access_key, secret_key, None))
                    .build();

                let client = aws_sdk_s3::Client::from_conf(config);

                Ok(DeserializableBucket {
                    client: client,
                    bucket_name: bucket_name,
                })
            }
        }

        const FIELDS: &[&str] = &["bucket_name", "endpoint", "access_key", "secret_key"];
        deserializer.deserialize_struct("DeserializableBucket", FIELDS, BucketVisitor)
    }
}

impl std::fmt::Debug for DeserializableBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeserializableBucket")
            .field("bucket_name", &self.bucket_name)
            .finish()
    }
}

impl DeserializableBucket {
    async fn get_cached_used_bytes(&self) -> Option<u64> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(".mergers3")
            .send()
            .await
        {
            Ok(output) => {
                let mut bytes = vec![];
                output
                    .body
                    .into_async_read()
                    .read_to_end(&mut bytes)
                    .await
                    .unwrap();
                let used_bytes = String::from_utf8(bytes).unwrap().parse::<u64>().unwrap();
                Some(used_bytes)
            }
            Err(_) => None,
        }
    }

    async fn calc_used_bytes(&self) -> u64 {
        let mut used_bytes = 0;
        let mut continuation_token: Option<String> = None;
        loop {
            let list_objects_output = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .expect("Failed to list objects in bucket");

            for object in list_objects_output.contents.as_ref().unwrap_or(&vec![]) {
                if object.key() == Some(".mergers3") {
                    continue;
                }
                used_bytes += object.size as u64;
            }

            let next_continuation_token = list_objects_output.next_continuation_token();
            if next_continuation_token.is_none() {
                break;
            }
            continuation_token = list_objects_output.next_continuation_token;
        }

        used_bytes
    }

    async fn put_capacity_file(&self, used_bytes: u64) {
        // write the used capacity to the .mergers3 file
        // this might change the capacity of the bucket, but it should only be a few bytes
        // so ignore the .mergers3 file when calculating the capacity
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(".mergers3")
            .body(ByteStream::from(used_bytes.to_string().as_bytes().to_vec()))
            .send()
            .await
            .expect(
                format!(
                    "Failed to put .mergers3 file in bucket {}",
                    self.bucket_name
                )
                .as_str(),
            );
    }

    async fn put_capacity_file_if_missing(&self) {
        if self.get_cached_used_bytes().await.is_none() {
            self.put_capacity_file(self.calc_used_bytes().await).await;
        }
    }

    async fn check_capacity_file(&self) {
        let cached_used_bytes = self.get_cached_used_bytes().await;
        if cached_used_bytes.is_some() {
            let used_bytes = self.calc_used_bytes().await;
            if used_bytes != cached_used_bytes.unwrap() {
                warn!("Bucket {} has a .mergers3 file with a different size ({}) than the actual size ({})", self.bucket_name, cached_used_bytes.unwrap(), used_bytes);
            }
        } else {
            warn!("Bucket {} does not have a .mergers3 file", self.bucket_name);
        }
    }
}

// https://stackoverflow.com/a/46755370
fn bytes_from_gb_f64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let gb = f64::deserialize(deserializer)?;
    Ok((gb * 1000.0 * 1000.0 * 1000.0) as u64)
}

#[derive(Debug, Deserialize)]
pub struct SourceBucket {
    #[serde(flatten)]
    bucket: DeserializableBucket,
    #[serde(rename = "capacity_gb", deserialize_with = "bytes_from_gb_f64")]
    capacity_bytes: u64,

    #[serde(skip)]
    // only one thread can be making size-modifying requests at a time
    // otherwise, the capacity cache file value will be subject to race conditions
    size_mutex: Mutex<()>,
}

pub enum SourceBucketDeleteObjectError {
    DeleteObject(SdkError<DeleteObjectError>),
    HeadObject(SdkError<HeadObjectError>),
}

impl From<SdkError<DeleteObjectError>> for SourceBucketDeleteObjectError {
    fn from(e: SdkError<DeleteObjectError>) -> Self {
        SourceBucketDeleteObjectError::DeleteObject(e)
    }
}

impl From<SdkError<HeadObjectError>> for SourceBucketDeleteObjectError {
    fn from(e: SdkError<HeadObjectError>) -> Self {
        SourceBucketDeleteObjectError::HeadObject(e)
    }
}

impl SourceBucket {
    pub fn get_name(&self) -> &str {
        &self.bucket.bucket_name
    }

    pub async fn has_bytes(&self, min_available_bytes: u64) -> bool {
        let used_bytes = match self.bucket.get_cached_used_bytes().await {
            Some(used_bytes) => used_bytes,
            None => self.bucket.calc_used_bytes().await,
        };

        self.capacity_bytes - used_bytes >= min_available_bytes
    }

    async fn contains_object(&self, key: &str) -> Option<HeadObjectOutput> {
        match self
            .bucket
            .client
            .head_object()
            .bucket(&self.bucket.bucket_name)
            .key(key)
            .send()
            .await
        {
            Ok(res) => Some(res),
            Err(_) => None,
        }
    }

    pub async fn update_used_bytes(&self, delta_bytes: i64) {
        let _guard = self.size_mutex.lock().await;

        let used_bytes = match self.bucket.get_cached_used_bytes().await {
            Some(used_bytes) => used_bytes,
            None => self.bucket.calc_used_bytes().await,
        };

        self.bucket
            .put_capacity_file(used_bytes.checked_add_signed(delta_bytes).unwrap())
            .await;
    }

    pub async fn head_object(
        &self,
        input: &HeadObjectInput,
    ) -> Result<aws_sdk_s3::output::HeadObjectOutput, SdkError<HeadObjectError>> {
        let mut input = input.clone();
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        self.bucket
            .client
            .head_object()
            .key(" ") // required so that the operation builds successfully
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await
    }

    pub async fn get_object(
        &self,
        input: &GetObjectInput,
    ) -> Result<aws_sdk_s3::output::GetObjectOutput, SdkError<GetObjectError>> {
        let mut input = input.clone();
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        self.bucket
            .client
            .get_object()
            .key(" ") // required so that the operation builds successfully
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await
    }

    pub async fn delete_object_and_update_size(
        &self,
        input: &DeleteObjectInput,
    ) -> Result<aws_sdk_s3::output::DeleteObjectOutput, SourceBucketDeleteObjectError> {
        let obj_size = self
            .bucket
            .client
            .head_object()
            .bucket(&self.bucket.bucket_name)
            .key(input.key.as_ref().unwrap())
            .send()
            .await?
            .content_length;

        let mut input = input.clone();
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        let resp = self
            .bucket
            .client
            .delete_object()
            .key(" ")
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await?;

        self.update_used_bytes(-obj_size).await;
        Ok(resp)
    }

    pub async fn list_objects(
        &self,
        input: &ListObjectsInput,
    ) -> Result<aws_sdk_s3::output::ListObjectsOutput, SdkError<ListObjectsError>> {
        let mut input = input.clone();
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        self.bucket
            .client
            .list_objects()
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await
    }

    pub async fn put_object_and_update_size(
        &self,
        mut input: PutObjectInput, // not a reference because PutObjectInput doesn't implement Clone for some reason
        body_bytes: u64,
    ) -> Result<aws_sdk_s3::output::PutObjectOutput, SdkError<PutObjectError>> {
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        let resp = self
            .bucket
            .client
            .put_object()
            .key(" ")
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await?;

        self.update_used_bytes(i64::try_from(body_bytes).unwrap())
            .await;
        Ok(resp)
    }

    pub async fn copy_object_and_update_size(
        &self,
        input: &CopyObjectInput,
        body_bytes: u64,
    ) -> Result<aws_sdk_s3::output::CopyObjectOutput, SdkError<CopyObjectError>> {
        let mut input = input.clone();
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        let resp = self
            .bucket
            .client
            .copy_object()
            .copy_source(" ")
            .key(" ")
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await?;

        self.update_used_bytes(i64::try_from(body_bytes).unwrap())
            .await;
        Ok(resp)
    }
}

#[derive(Debug, Deserialize)]
pub struct MergedBucket(Vec<SourceBucket>);

pub type BucketFunc<'a, A, B, E> = fn(&'a SourceBucket, &'a A) -> BoxFuture<'a, Result<B, E>>;

impl MergedBucket {
    async fn create_capacity_files(&self) {
        let futures = self
            .0
            .iter()
            .map(|source_bucket| source_bucket.bucket.put_capacity_file_if_missing());
        futures::future::join_all(futures).await;
    }

    async fn check_capacity_files(&self) {
        let futures = self
            .0
            .iter()
            .map(|source_bucket| source_bucket.bucket.check_capacity_file());
        futures::future::join_all(futures).await;
    }

    pub async fn get_source_bucket(&self, min_available_bytes: u64) -> Option<&SourceBucket> {
        let futures = self.0.iter().map(|source_bucket| {
            source_bucket
                .has_bytes(min_available_bytes)
                .map(
                    move |success| {
                        if success {
                            Some(source_bucket)
                        } else {
                            None
                        }
                    },
                )
        });
        select_some(futures).await
    }

    pub async fn find_object_source(&self, key: &str) -> Option<(&SourceBucket, HeadObjectOutput)> {
        let futures = self.0.iter().map(|source_bucket| {
            source_bucket
                .contains_object(key)
                .map(move |output| match output {
                    Some(output) => Some((source_bucket, output)),
                    None => None,
                })
        });
        select_some(futures).await
    }

    pub async fn first_found<'a, A, B, E>(
        &'a self,
        input: &'a A,
        f: BucketFunc<'a, A, B, E>,
    ) -> Result<B, Vec<E>> {
        // return the first result that succeeds
        let futures = self.0.iter().map(|source_bucket| f(source_bucket, input));
        select_ok(futures).await
    }

    pub async fn all<'a, A, B, E>(
        &'a self,
        input: &'a A,
        f: BucketFunc<'a, A, B, E>,
    ) -> (Vec<B>, Vec<E>) {
        let futures = self.0.iter().map(|source_bucket| f(source_bucket, input));
        let mut successes = Vec::new();
        let mut errs = Vec::new();
        for res in futures::future::join_all(futures).await {
            if let Ok(res) = res {
                successes.push(res);
            } else if let Err(err) = res {
                errs.push(err);
            }
        }
        (successes, errs)
    }

    pub fn as_content(&self) -> &Vec<SourceBucket> {
        &self.0
    }
}

pub async fn read_config() -> BTreeMap<String, MergedBucket> {
    let rdr = File::open("config.yaml").expect("Failed to open config.yaml");
    let buckets: BTreeMap<String, MergedBucket> =
        serde_yaml::from_reader(rdr).expect("Failed to parse config.yaml");

    debug!("Merged bucket configuration: {:#?}", buckets);

    debug!("Creating .mergers3 capacity files...");
    let futures = buckets
        .values()
        .map(|bucket| bucket.create_capacity_files());
    futures::future::join_all(futures).await;
    debug!(".mergers3 capacity files created.");

    println!("Checking for incorrect capacity files...");
    let futures = buckets.values().map(|bucket| bucket.check_capacity_files());
    futures::future::join_all(futures).await;

    buckets
}
