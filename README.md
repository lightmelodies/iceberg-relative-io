# Iceberg Relative File IO
This project implements relative catalog and file-io that allow relative path in table metadata.

## How to use
Set `catalog-impl` to `org.apache.iceberg.hadoop.HadoopRelativeCatalog` or `org.apache.iceberg.jdbc.JdbcRelativeCatalog`, and you are done.

```
spark.sql.catalog.test                      org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.test.catalog-impl         org.apache.iceberg.hadoop.HadoopRelativeCatalog
spark.sql.catalog.test.warehouse            file:///iceberg/warehouse/
```

```SQL
CREATE TABLE test.my_ns.my_table (id bigint, data string, category string);

INSERT INTO test.my_ns.my_table VALUES (1, "Pizza", "orders");

SELECT * FROM test.my_ns.my_table;
```

Now iceberg will create `metadata.json` with a `location` relative to `warehouse`:

```json
{
  "format-version" : 2,
  "table-uuid" : "dcbbd573-4ec1-45c1-8216-e999822af202",
  "location" : "my_ns/my_table",
  "last-sequence-number" : 1,
  "last-updated-ms" : 1727488832656,
  "last-column-id" : 3,
  "current-schema-id" : 0,
  "schemas" : [ {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "id",
      "required" : false,
      "type" : "long"
    }, {
      "id" : 2,
      "name" : "data",
      "required" : false,
      "type" : "string"
    }, {
      "id" : 3,
      "name" : "category",
      "required" : false,
      "type" : "string"
    } ]
  } ],
  "default-spec-id" : 0,
  "partition-specs" : [ {
    "spec-id" : 0,
    "fields" : [ ]
  } ],
  "last-partition-id" : 999,
  "default-sort-order-id" : 0,
  "sort-orders" : [ {
    "order-id" : 0,
    "fields" : [ ]
  } ],
  "properties" : {
    "owner" : "root",
    "write.parquet.compression-codec" : "zstd"
  },
  "current-snapshot-id" : 3059744530731669432,
  "refs" : {
    "main" : {
      "snapshot-id" : 3059744530731669432,
      "type" : "branch"
    }
  },
  "snapshots" : [ {
    "sequence-number" : 1,
    "snapshot-id" : 3059744530731669432,
    "timestamp-ms" : 1727488832656,
    "summary" : {
      "operation" : "append",
      "spark.app.id" : "local-1727487543838",
      "added-data-files" : "1",
      "added-records" : "1",
      "added-files-size" : "909",
      "changed-partition-count" : "1",
      "total-records" : "1",
      "total-files-size" : "909",
      "total-data-files" : "1",
      "total-delete-files" : "0",
      "total-position-deletes" : "0",
      "total-equality-deletes" : "0",
      "engine-version" : "3.5.1",
      "app-id" : "local-1727487543838",
      "engine-name" : "spark",
      "iceberg-version" : "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)"
    },
    "manifest-list" : "my_ns/my_table/metadata/snap-3059744530731669432-1-32469e92-4bc7-4e6f-84b0-9df009dfa743.avro",
    "schema-id" : 0
  } ],
  "statistics" : [ ],
  "partition-statistics" : [ ],
  "snapshot-log" : [ {
    "timestamp-ms" : 1727488832656,
    "snapshot-id" : 3059744530731669432
  } ],
  "metadata-log" : [ {
    "timestamp-ms" : 1727487584337,
    "metadata-file" : "my_ns/my_table/metadata/v1.metadata.json"
  } ]
}
```

The warehouse is free to move anywhere. You can even read it with a different protocol, e.g. S3:

```
spark.sql.catalog.test                      org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.test.catalog-impl         org.apache.iceberg.hadoop.HadoopRelativeCatalog
spark.sql.catalog.test.warehouse            s3a://iceberg/warehouse

# config s3 related properties here
spark.hadoop.fs.s3a.endpoint                ...
```

For `HadoopRelativeCatalog`, you may need to set `fs.verfiy-checksum` to `false` if you mix posix and S3 protocols.
Otherwise reading `version-hint.text` may fail because S3 does not update the checksum file.

## How it works
The`HadoopRelativeCatalog` is slightly different from `HadoopCatalog`. It sets `defaultWarehouseLocation` to a relative
location. The absolute location will be constructed by `RelativeFileIO`. The same trick applies for `JdbcRelativeCatalog`
and should be able to work with other catalogs.