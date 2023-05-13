# Persisty Data

This is an addition to the [persisty](https://github.com/tofarr/persisty) framework for handling binary data 
(File Uploads). The objective is to be compatible with third party services, including ones that use signed
URLs for posting / reading (Such as S3).

* [DataItems](persisty_data/data_item_abc.py) represent the storage of binary data, with methods for reading and writing
  as well as obtaining meta such as file size, last modified and etag
* [DataStores](persisty_data/data_store_abc.py) store data items, with higher level methods for copying efficiently
* [DataStoreFactories](persisty_data/data_store_factory_abc.py) handle authorization when creating create data stores,
  as well as providing methods for generating presigned urls meaning that the server may be bypassed completely in some
  implementations (such as S3)

## Implementations

### Chunk

The chunk implementation breaks up an item into [chunks](persisty_data/chunk.py) smaller than preset size and storing
these using regular persisty.

### Directory

The [Directory Store](persisty_data/directory_data_store.py) stores data items as files within a directory on a hosted
file system.

### S3

The [S3 Store](persisty_data/s3_data_store.py) stores data items as entries in an S3 bucket. It is unique in the stock
implementations in that it also provides an [S3 Data Store Factory](persisty_data/s3_data_store_factory.py) rather
than relying on the [Hosted Data Store Factory](persisty_data/hosted_data_store_factory.py) used by other
implementations for providing pre-signed read urls and [Upload Forms](persisty_data/upload_form.py).

* Reading Data
* Writing data
* Strategy for bypassing server using Signed URLs
* Example

## Example

[A variant of the persisty messager example application demonstrates file upload.](examples/messager)

## Installing local development dependencies

```
python setup.py install easy_install "persisty-data[dev]"
```

## Release Procedure

![status](https://github.com/tofarr/persisty-data/actions/workflows/quality.yml/badge.svg?branch=main)

The typical process here is:
* Create a PR with changes. Merge these to main (The `Quality` workflows make sure that your PR
  meets the styling, linting, and code coverage standards).
* New releases created in github are automatically uploaded to pypi
