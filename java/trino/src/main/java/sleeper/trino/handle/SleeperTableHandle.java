package sleeper.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This table handle holds details of the table name, the column handles and any {@link TupleDomain} that is to be used
 * to filter the results from the table when it is scanned.
 */
public class SleeperTableHandle implements ConnectorTableHandle {
    private final SchemaTableName schemaTableName;
    private final List<SleeperColumnHandle> sleeperColumnHandleListInOrder;
    private final TupleDomain<ColumnHandle> tupleDomain;

    @JsonCreator
    public SleeperTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                              @JsonProperty("sleeperColumnHandleListInOrder") List<SleeperColumnHandle> sleeperColumnHandleListInOrder,
                              @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain) {
        this.schemaTableName = requireNonNull(schemaTableName);
        this.sleeperColumnHandleListInOrder = requireNonNull(sleeperColumnHandleListInOrder);
        this.tupleDomain = requireNonNull(tupleDomain);
    }

    public SleeperTableHandle(SchemaTableName schemaTableName,
                              List<SleeperColumnHandle> sleeperColumnHandleListInOrder) {
        this(schemaTableName, sleeperColumnHandleListInOrder, TupleDomain.all());
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public List<SleeperColumnHandle> getSleeperColumnHandleListInOrder() {
        return sleeperColumnHandleListInOrder;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return tupleDomain;
    }

    /**
     * A convenience method to return this handle as a {@link ConnectorTableMetadata} object.
     *
     * @return The {@link ConnectorTableMetadata} object.
     */
    public ConnectorTableMetadata toConnectorTableMetadata() {
        List<ColumnMetadata> columnMetadataList = this.sleeperColumnHandleListInOrder.stream()
                .map(SleeperColumnHandle::toColumnMetadata)
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(this.schemaTableName, columnMetadataList);
    }

    /**
     * A convenience method to return this handle as a {@link TableColumnsMetadata} object.
     *
     * @return The {@link TableColumnsMetadata} object.
     */
    public TableColumnsMetadata toTableColumnsMetadata() {
        return TableColumnsMetadata.forTable(schemaTableName, toConnectorTableMetadata().getColumns());
    }

    /**
     * Replace the {@link TupleDomain} in this handle with a new tupledomain and return a copy with all other fields
     * intact.
     *
     * @param newTupleDomain The {@link TupleDomain} to use in the new copy.
     * @return The copied {@link SleeperTableHandle}.
     */
    public SleeperTableHandle withTupleDomain(TupleDomain<ColumnHandle> newTupleDomain) {
        return new SleeperTableHandle(schemaTableName, sleeperColumnHandleListInOrder, newTupleDomain);
    }

    public List<SleeperColumnHandle> getColumnHandlesInCategoryInOrder(SleeperColumnHandle.SleeperColumnCategory category) {
        return this.getSleeperColumnHandleListInOrder()
                .stream()
                .filter(sleeperColumnHandle ->
                        sleeperColumnHandle.getColumnCategory() == category)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SleeperTableHandle that = (SleeperTableHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(sleeperColumnHandleListInOrder, that.sleeperColumnHandleListInOrder) &&
                Objects.equals(tupleDomain, that.tupleDomain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaTableName, sleeperColumnHandleListInOrder, tupleDomain);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("sleeperColumnHandleList", sleeperColumnHandleListInOrder)
                .add("tupleDomain", tupleDomain)
                .toString();
    }
}
