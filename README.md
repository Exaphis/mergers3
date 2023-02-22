# mergers3

[mergerfs](https://github.com/trapexit/mergerfs), but dumb. Born out of the desire to
merge multiple S3 buckets into one.

## Supported operations

Free space is kept in a bucket object called '.mergers3'. This object is created when
the bucket is first used. It is updated when an object is added or removed.

### ListObjects

ListObjects will list all objects in all buckets. This is done by merging the results
of ListObjectsV2 from each bucket.

### DeleteObject

DeleteObject will delete the object from all buckets.

### CopyObject

CopyObject will copy the object from one prefix to another. It will first attempt to
copy the object to the same source bucket. If that fails, it will then copy it to a different
source bucket via PutObject.

### HeadObject

HeadObject will return the object's metadata from the first bucket found that contains the object.

### GetObject

HeadObject will return the object from the first bucket found that contains the object.

### PutObject

PutObject will put the object into the first bucket that has space.