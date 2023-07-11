package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface QueryProperties {
    UserDefinedInstanceProperty MAXIMUM_CONNECTIONS_TO_S3_FOR_QUERIES = Index.propertyBuilder("sleeper.query.s3.max-connections")
            .description("The maximum number of simultaneous connections to S3 from a single query runner. This is separated " +
                    "from the main one as it's common for a query runner to need to open more files at once.")
            .defaultValue("1024")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB = Index.propertyBuilder("sleeper.query.processor.memory")
            .description("The amount of memory in MB for the lambda that executes queries.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.query.processor.timeout.seconds")
            .description("The timeout for the lambda that executes queries in seconds.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.query.processor.state.refresh.period.seconds")
            .description("The frequency with which the query processing lambda refreshes its knowledge of the system state " +
                    "(i.e. the partitions and the mapping from partition to files), in seconds.")
            .defaultValue("60")
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE = Index.propertyBuilder("sleeper.query.processor.results.batch.size")
            .description("The maximum number of records to include in a batch of query results send to " +
                    "the results queue from the query processing lambda.")
            .defaultValue("2000")
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_PROCESSOR_LAMBDA_RECORD_RETRIEVAL_THREADS = Index.propertyBuilder("sleeper.query.processor.record.retrieval.threads")
            .description("The size of the thread pool for retrieving records in a query processing lambda.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_TRACKER_ITEM_TTL_IN_DAYS = Index.propertyBuilder("sleeper.query.tracker.ttl.days")
            .description("This value is used to set the time-to-live on the tracking of the queries in the DynamoDB-based query tracker.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty QUERY_RESULTS_BUCKET_EXPIRY_IN_DAYS = Index.propertyBuilder("sleeper.query.results.bucket.expiry.days")
            .description("The length of time the results of queries remain in the query results bucket before being deleted.")
            .defaultValue("7")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.QUERY)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_ROW_GROUP_SIZE = Index.propertyBuilder("sleeper.default.query.results.rowgroup.size")
            .description("The default value of the rowgroup size used when the results of queries are written to Parquet files. The " +
                    "value given below is 8MiB. This value can be overridden using the query config.")
            .defaultValue("" + (8 * 1024 * 1024)) // 8 MiB
            .propertyGroup(InstancePropertyGroup.QUERY).build();
    UserDefinedInstanceProperty DEFAULT_RESULTS_PAGE_SIZE = Index.propertyBuilder("sleeper.default.query.results.page.size")
            .description("The default value of the page size used when the results of queries are written to Parquet files. The " +
                    "value given below is 128KiB. This value can be overridden using the query config.")
            .defaultValue("" + (128 * 1024)) // 128 KiB
            .propertyGroup(InstancePropertyGroup.QUERY).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
