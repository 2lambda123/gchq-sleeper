package sleeper.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.query.QueryAssertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.trino.testutils.PopulatedSleeperExternalResource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleKeyValueSchemaInsertIT {
    private static final String TEST_TABLE_NAME = "mytable";
    private static final int NO_OF_RECORDS = 100;

    private static final List<PopulatedSleeperExternalResource.TableDefinition> TABLE_DEFINITIONS =
            ImmutableList.of(
                    new PopulatedSleeperExternalResource.TableDefinition(
                            TEST_TABLE_NAME,
                            generateSimpleSchema(),
                            Optional.empty(),
                            Optional.empty()));


    @ClassRule
    public static final PopulatedSleeperExternalResource populatedSleeperExternalResource =
            new PopulatedSleeperExternalResource(ImmutableMap.of(), TABLE_DEFINITIONS, Optional.empty());
    private static QueryAssertions assertions;

    private static Schema generateSimpleSchema() {
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new StringType()));
        schema.setValueFields(new Field("value", new StringType()));
        return schema;
    }

    private static Stream<Record> generateSimpleRecordStream() {
        return IntStream.range(0, NO_OF_RECORDS).mapToObj(recordNo -> {
            Record record = new Record();
            record.put("key", String.format("key-%09d", recordNo));
            record.put("value", String.format("val-%09d", recordNo));
            return record;
        });
    }

    @BeforeClass
    public static void beforeClass() {
        assertions = populatedSleeperExternalResource.getQueryAssertions();
        String valuesTerms = generateSimpleRecordStream()
                .map(record -> String.format("('%s', '%s')", record.get("key"), record.get("value")))
                .collect(Collectors.joining(","));
        populatedSleeperExternalResource.getQueryAssertions().execute(
                String.format("INSERT INTO sleeper.default.%s VALUES %s", TEST_TABLE_NAME, valuesTerms));
    }

    @Test
    public void testEq() {
        assertThat(assertions.query(String.format(
                "SELECT key, value FROM sleeper.default.%s WHERE key = 'key-000000000'", TEST_TABLE_NAME)))
                .matches("VALUES (CAST ('key-000000000' AS VARCHAR), CAST('val-000000000' AS VARCHAR))");
    }

    @Test
    public void testCountMinMax() {
        assertThat(assertions.query(String.format(
                "SELECT MIN(key), MAX(key), MIN(value), MAX(value), COUNT(*) " +
                        "FROM sleeper.default.%s WHERE key LIKE 'key-%%'", TEST_TABLE_NAME)))
                .matches(String.format("VALUES (" +
                                "CAST ('key-%09d' AS VARCHAR), " +
                                "CAST ('key-%09d' AS VARCHAR), " +
                                "CAST ('val-%09d' AS VARCHAR), " +
                                "CAST ('val-%09d' AS VARCHAR), " +
                                "CAST (%d AS BIGINT))",
                        0, NO_OF_RECORDS - 1, 0, NO_OF_RECORDS - 1, NO_OF_RECORDS));
    }
}
