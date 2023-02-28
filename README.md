# mergers3

## What?

[mergerfs](https://github.com/trapexit/mergerfs), but for S3. Born out of the desire to
merge multiple small S3 buckets into one larger bucket.
Supports limits on the size of the underlying source buckets.

## Why?

I have a bunch of free tier S3 buckets, but none of them are really big enough to store
anything useful. So I merged them into one big bucket.

You could (and should instead) use [StableBit CloudDrive + DrivePool](https://stablebit.com/DrivePool/Features)
to merge multiple buckets into a single drive. But why pay $60 when you can... spend a
week of your precious life to write a janky program...

## How?

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