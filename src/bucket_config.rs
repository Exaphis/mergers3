use std::{collections::BTreeMap, fs::File};

use aws_sdk_s3::{
    error::{DeleteObjectError, GetObjectError},
    input::{DeleteObjectInput, GetObjectInput},
    output::GetObjectOutput,
    presigning::config::PresigningConfig,
    types::{ByteStream, SdkError},
    Client, Credentials, Region,
};
use aws_smithy_http::operation::error::BuildError;
use hyper::Response;
use log::debug;
use serde::{
    de::{MapAccess, Visitor},
    Deserialize, Deserializer,
};

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
    pub async fn create_capacity_file(&self) {
        if self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(".mergers3")
            .send()
            .await
            .is_ok()
        {
            // .mergers3 file already exists, skip
            return;
        }

        // calculate the used capacity of bucket
        let mut used_bytes = 0;
        let mut continuation_token: Option<String> = None;
        loop {
            let list_objects_output = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .continuation_token(continuation_token.unwrap_or_default())
                .send()
                .await
                .expect("Failed to list objects in bucket");
            for object in list_objects_output.contents.unwrap() {
                used_bytes += object.size;
            }
            if list_objects_output.next_continuation_token.is_none() {
                break;
            }
            continuation_token = list_objects_output.next_continuation_token;
        }

        // write the used capacity to the .mergers3 file
        // this might change the capacity of the bucket, but it should only be a few bytes
        self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(".mergers3")
            .body(ByteStream::from(used_bytes.to_string().as_bytes().to_vec()))
            .send()
            .await
            .expect(
                format!(
                    "Failed to create .mergers3 file in bucket {}",
                    self.bucket_name
                )
                .as_str(),
            );
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
}

impl SourceBucket {
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

    pub async fn delete_object(
        &self,
        input: &DeleteObjectInput,
    ) -> Result<aws_sdk_s3::output::DeleteObjectOutput, SdkError<DeleteObjectError>> {
        let mut input = input.clone();
        input.bucket = Some(self.bucket.bucket_name.clone());
        let operation = input.make_operation(self.bucket.client.conf()).await;

        self.bucket
            .client
            .delete_object()
            .customize()
            .await
            .unwrap()
            .map_operation(|_| operation)
            .unwrap()
            .send()
            .await
    }
}

#[derive(Debug, Deserialize)]
pub struct MergedBucket(Vec<SourceBucket>);

impl MergedBucket {
    pub async fn create_capacity_files(&self) {
        let futures = self
            .0
            .iter()
            .map(|source_bucket| source_bucket.bucket.create_capacity_file());
        futures::future::join_all(futures).await;
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
    buckets
}
