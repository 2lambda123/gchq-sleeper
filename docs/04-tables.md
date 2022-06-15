Tables
======

A Sleeper instance contains one or more tables. Each a table has four important
properties: a name, a schema, its own S3 bucket for storing data for that
table, and its own state store for storing metadata about the table
(this is normally stored in DynamoDB tables, each Sleeper table will have
its own DynammoDB tables for storing the metadata). All other resources
for the instance, such as ECS clusters and lambda functions, are shared
across all the tables.

## The metadata store
Each table has metadata associated to it. This metadata is stored in a
StateStore and consists of information about the files that are in the
system, and the partitions. Sleeper has two options for the storage of
this metadata: the DynamoDBStateStore and the S3StateStore. The default
option is the DynamoDBStateStore.

The DynamoDBStateStore stores the metadata in 3 DynamoDB tables. This
allows quick updates and retrieval of the metadata. When a compaction
job finishes, it needs to update the metadata about the files. Specifically
it needs to mark the files that were input to the compaction job as being
ready for garbage collection and it needs to add the output file as a new
active file. This needs to happen in a transaction to ensure that clients
always see a consistent view of the metadata. DynamoDB transactions are
limited in size and this limits the number of files that can be read in a
compaction to 11.

The S3StateStore stores the metadata in Parquet files within the S3 bucket
used to store the table's data. When an update happens, a new file is
written containing the updated version of the metadata. To ensure that
two processes cannot both update the metadata at the same time leading
to a conflict, DynamoDB is used as a lightweight consistency layer.
Specifically an item in Dynamo is used to record the current version of
the metadata. When a client attempts to write a new version of the
metadata, it performs a conditional update to that item (the condition
is that the value is still the value the client based its update on).
If the update fails it means that another client updated it first, and
in this case the update is retried. As all the metadata is rewritten
on each update, there is no limit to the number of items that can be
read in a compaction job.

Currently, the best tested option is the DynamoDBStateStore. This is
likely to be the best option if you have a large number of processes
inserting data in parallel.
