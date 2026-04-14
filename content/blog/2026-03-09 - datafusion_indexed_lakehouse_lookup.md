+++
title = "Datafusion — fast lakehouse querying using external index"
date = 2026-03-09
[taxonomies]
tags=["Rust", "Datafusion"]
+++

In this post I would like to tell about how we used Apache DataFusion to implement **fast** querying of **small** portions of data from our data lakehouse.

## Background

I work for a music publisher that collaborates with different media platforms.
We have a lot of historical data about media content that has been sent to each of partners. This data is mostly used for generating reports.

In a nutshell our system looks like this:

![](/img/data_lakehouse-reporting_flow.svg)

<center>(arrows show the direction where the data flows)</center>

The data is stored in data lakehouse: parquet files with Hive-like partitioning in S3. At the moment we have roughly 30 TB of data.
Since reports are usually partner-specific and require aggregation over a certain period, we partition the data by partner name, year and month.


```
s3://storage-bucket/
 ├──table1/
 │   ├─ parnter=ABC/
 │   │   ├─ year=2020/
 │   │   │   ├─ month=1/
 │   │   │   │   ├─ part-0001.snappy.parquet
 │   │   │   │   └─ part-0002.snappy.parquet
 │   │   │   ├─ month=2/
 │   │   │   │   ├─ part-0001.snappy.parquet
 │   │   │   │   └─ part-0002.snappy.parquet
 │   │   │   └─ month=12/
 │   │   │       └─ part-0001.snappy.parquet
 │   │   └─ year=2025/
 │   │       └─ month=1/
 │   │           ├─ part-0001.snappy.parquet
 │   │           └─ part-0002.snappy.parquet
 │   └─ parnter=XYZ/
 │       ├─ year=2020/
 │       │   ├─ month=1/
 …       …   …
```

For simplicity, in this article I will use a very simplified version of our table called `product`.
This table stores general information about products (media asset sent to a partner).

The table consists of following columns:

|       Column        |    Type     |                 Description                 |       Stored as        |
| ------------------- | ----------- | ------------------------------------------- | ---------------------- |
| `product_id`        | `INT64`     | ID of product sent to a customer            | column in parquet file |
| `upc`               | `Utf8`      | UPC of the sent product                     | column in parquet file |
| `created_timestamp` | `TIMESTAMP` | Exact time when product has been created    | column in parquet file |
| `title`             | `Utf8`      | Title of the product                        | column in parquet file |
| `partner`           | `Utf8`      | 3 characters partner code                   | Hive partition         |
| `year`              | `INT`       | year at which the product has been created  | Hive partition         |
| `month`             | `INT`       | month at which the product has been created | Hive partition         |



## Problem

As I've mentioned, the data is mostly used for generating reports, which are implemented as heavy analytical queries that are executed either with Apache Spark or Amazon Athena.
But we also have cases when we need to quickly query small portions of data.
For example, get the UPC for a product with given Product ID.

Utilizing Athena for such point lookups would be costly and slow. For example, following query takes up to 30 seconds when executed with Athena, plus it does tons of unnecessary I/O by scanning hundreds of gigabytes.

```sql
SELECT title, upc
FROM product
WHERE product_id = 384209819 
```

The standard solution for this porblem is to introduce an index that allows to quickly identify the partition containing the desired record. But neither Spark not Athena natively supports these types of custom indexes.
And this is where Datafusion came to help.


## About Datafusion

If you are not familiar with [Datafusion](https://datafusion.apache.org/), then think of it as a framework for building query engines and databases.

Imagine you write your own query engine for executing SQL queries on some structured files. You would have to implement following pipeline:

![](/img/sql_execution_pipeline.svg)

The Datafusion is a query engine that already provides all these components, along with adapters for popular data formats like CSV, JSON, AVRO, Parquet.

But what maskes Datafusion so unique is that it allows you to customize any step of the data querying pipeline:

- Want a custom query language instead of SQL? Just implelment a parser for it and reuse other components.
- Want to plug in a custom SQL function? Datafusion provides an extension point for that.
- Want to query tables that are represented by your own custom format? It is also very easy to implement.

Just to give you a feeling how datafusion API looks like, let's make a simple SQL query on a local parquet file:

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
  let ctx = SessionContext::new(); // Datafusion context

  // Registering parquet file as a table, which will allow us to query it in SQL statement
  ctx.register_parquet("product", "data_dir/product/partition-0001.parquet", ParquetReadOptions::default()).await?;

  // Parse SQL into a logical plan (dataframe is just a wrapper for a logical plan)
  let df = ctx.sql("SELECT * FROM product LIMIT 100").await?;

  // Turn logical plan into a physical plan, then execute it, and print the result.
  df.show().await?;

  Ok(())
}
```

As you can see, out of the box Datafusion already provides a lot of functionality.

Now let's look at how we can build an index and then leverage Datafusion to quickly query the `product` table.

## Indexing data by Product ID

As we've said, the biggest problem is that we don't know in what partition (S3 folder) to search for a file that contains a product record with the required product ID. That's why we need an index that stores mapping Product ID ➜ Partition.

E.g. `1111 => (partner=ABC, year=2025, month=1)`, `2222 => (partner=XYZ, year=2026, month=2)`, etc.

![](/img/product_id_indexing.svg)

I have chosen to store the index in RocksDB, since it is fast, cheap and relatively easy to manage.

The indexing works as following:

1) Fisrts Spark reads and processes the source data
2) Then when Spark writes the output to the data lakehouse, it also produces an SQS message with information about what partitions the output parquet files have been written to.
3) A separate indexer application consumes this SQS message and then:
4) Indexer walks through updated partitions, reads newly added parquet files and takes Product ID values from them
5) Adds (Product ID ➜ Partition) records to the index

![](/img/data_lakehouse-indexing_flow.svg)

The interval between Spark microbatches is 6 minutes which results in less than 15 thousand SQS calls per month.


## Querying with index

So, as a first step I've created an indexer program that iterates over parquet files, collects Product ID and fills the RocksDB index.

Next I took Datafusion and similar to [this example](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) I've implemented a custom [TableProvider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) that uses RocksDB index to resolve ProductID into a data lakehouse partition.

![](/img/data_lakehouse-query_flow.svg)

## TableProvider

Now let's quickly look at the [TableProvider](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html) trait, which is the main Datafusion trait that you must implement if you want to introduce a custom implementation of a table.

When implementing `TableProvider` for our type, there are the two most important methods to be implemented:
* [**schema**() -> Schema](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.schema) — provides the table's schema as an object of [arrow_schema::schema::Schema](https://docs.rs/arrow-schema/latest/arrow_schema/struct.Schema.html).
* [**scan**(projection, filters, limits) -> dyn ExecutionPlan](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.scan) — provides an object of a type that implements [ExecutionPlan](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html) which represents a node in the plysical plan tree.

The following diagram shows traits and standard implementations that we need for making a custom indexed table provider.

![](/img/table_provider-principle.svg)

<center>(some signatures are simplified to fit into the diagram)</center>

As you can see, we need following types:

* [DataSourceExec](https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.DataSourceExec.html) — implementation of the `ExecutionPlan` trait that reads the data with the help of an object which type implements [DataSource](https://docs.rs/datafusion/latest/datafusion/datasource/source/trait.DataSource.html).
* [FileScanConfig](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html) — implementation of the `DataSource` trait that allows to read data stored as files in an object store. Relies on an object of a type that implements [FileSource](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/trait.FileSource.html) to work with exact file format.
* [ParquetSource](https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.ParquetSource.html) — implementation of the `FileSource` trait for reading parquet files.

## Table provider implementation

Now let's look at the implementation of `TableProvider` that uses the index to read parquet files only from partitions that contain required products.

(The example is simplified to be focuses on the index part, but if you do something similar, you may take in account things like Hive-partitioning, Iceberg support, interaction with a catalog. You may find a lot of useful in the source code for `datafusion::datasource::listing::ListingTable` struct)

`src/indexed_table.rs`:

```rust,linenos,name=src/indexed_table.rs
/// Out table that uses the index for parquet files lookup.
struct IndexedParquetTable {
  schema: Arc<Schema>,

  // Custom trait for resolving Datafusion predicates to S3 partitions folder
  partition_resolver: Arc<dyn PartitionResolver + Send + Sync>,

  // Custom trait for listing parquet files in given S3 folder
  partition_scanner: Arc<dyn PartitionScanner + Send + Sync> ,
}

#[async_trait::async_trait]
impl TableProvider for IndexedParquetTable {

    // This method will be called each time, when datafusion creates a physical plan
    // for a query that reads data from the indexed table (product).
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>, // list of columns that need to read
        filters: &[Expr],                // filtering conditions from WHERE block of an SQL query
        limit: Option<usize>,            // limit of rows to fetch
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // This call searches for a predicate on a column that is indexed with RocksDB.
        // In our case - product_id column.
        let partitions_to_scan = self.partition_resolver.get_partitions_for(&filters).await?;

        // Getting URLs to parquet files in S3 partition folder
        let obj_store = state.runtime_env().object_store(&self.object_store_url).unwrap();
        let partitioned_files = self.partition_scanner.list_files_for(&obj_store, &partitions_to_scan).await?;

        let projection_schema = match projection {
            Some(proj) => Arc::new(self.schema().project(&proj)?),
            None => self.schema(),
        };

        let source = ParquetSource::new(Arc::new(projection_schema))
            .with_pushdown_filters(true);

        let mut builder = FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(source));
        for partitioned_file in partitioned_files {
            builder = builder.with_file(partitioned_file);
        }

        let plan = DataSourceExec::from_data_source(builder.build());
        Ok(plan)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // means table can leverage filtering predicates
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    fn as_any(&self) ->  &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
```

Here I'm using two custom traits:

* `PartitionResolver` — defines method for resolving predicates like `product_id = 111` or `product_id IN (111, 222)` into partitions (S3 folders) where corresponding parquet files are stored.
* `PartitionScanner` — defines method for listing parquet files in a S3 directory.

Implementations of these traits are not very important in the scope of this article since we are more focused on the table provider itself, not on working with the RocksDB or S3.

`src/partition_resolver.rs`:

```rust
#[async_trait::async_trait]
trait PartitionResolver {
    /// Collects predicates specified for indexed columns (product_id)
    /// and resolves them into S3 partitions.
    async fn get_partitions_for(filters: &[Expr]) -> Result<Vec<String>>;
}

struct ProductPartitionResolver {
    index_storage: Arc<RocksIndexStorage>,
    s3_bucket_url: String,
}

#[async_trait::async_trait]
impl PartitionResolver for ProductPartitionResolver {
    ...
}
```

For example, assume that Product ID index contains mapping from `product_id = 111` to partition `(partner=ABC, year=2015, month=12)`. \
Then, if the `filters` argument contains an expression like: `Expr::BinaryExpr(BinaryExpr {left: Column("group_id"), op: Eq, right: Literal(Int64(111))})`, then `get_partitions_for` method will return `s3://my-bucket/tables/product/partner=ABC/year=2025/month=12/`.

`src/partition_scanner.rs`:

```rust
#[async_trait::async_trait]
trait PartitionScanner {
    /// List parquet files in S3 folders identified by given partitions.
    async fn list_files_for(object_strore: &dyn ObjectStore, partitions: &[AsRef<str>]) -> Result<Vec<String>>;
}

struct S3FileScanner;

#[async_trait::async_trait]
impl PartitionScanner for S3FileScanner {
    ...
}
```

Finally let's look at the usage exampple. In production we execute SQL queries from a Axum-based service, but for simplicity let's look how to use the indexed table from a simple CLI application.

`src/main.rs`:

```rust
mod indexed_table;
mod partition_resolver;
mod partition_scanner;

use std::sync::Arc;
use datafusion::execution::{context::SessionContext, object_store::ObjectStoreUrl};
use object_store::ObjectStore;
use storage::Storage;
use crate::{
    partition_resolver::ProductPartitionResolver,
    partition_scanner::S3FileScanner,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let s3_bucket = "our-storage-bucket";

    // Wrapper for RocksDb DB type.
    let index_storage = Arc::new(RocksIndexStorage::new());

    // Implementation of PartitionResolver trait we've defined above
    let partition_resolver = ProductPartitionResolver::new(index_storage.clone(), s3_bucket_url);

    // Implementation of PartitionScanner trait
    let file_scanner = S3FileScanner;

    let product_table = IndexedParquetTable {
        schema: build_product_table_schema(), // can return hardcoded schema, or take it from a catalog
        partition_resolver: Arc::new(partition_resolver),
        partition_scanner: Arc::new(file_scanner),
    };

    let ctx = SessionContext::new();

    // Registering our indexed table
    ctx.register_table("product", Arc::new(product_table))?;

    // Registering the object store, so it could be used by the PartitionScanner
    ctx.register_object_store(
            &url::Url::parse(format!("s3://{s3_bucket}/")).unwrap(),
            AmazonS3Builder::from_env().with_bucket_name(s3_bucket).build().unwrap(),
        );

    let df: Dataframe = ctx.sql(r#"
            SELECT title, upc, created_timestamp
            FROM product
            WHERE product_id = 384209819
        "#).await?;

    df.show().await?;

    Ok(())
}
```
The program output should look like:

```
+---------+---------+--------------------------------+
| title   | upc     |created_timestamp               |
+---------+---------+--------------------------------+
| title 1 | G010500A| 2026-02-14T11:45:44.721970738Z |
+---------+---------+--------------------------------+
```

## Conclusion

As the result, for a query like:

```sql
SELECT title, upc
FROM product
WHERE product_id = 384209819 
```

we were able to shorten the execution time from ~30 seconds to ~0.6 seconds.

