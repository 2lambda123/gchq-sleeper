package sleeper.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import sleeper.trino.handle.SleeperColumnHandle;
import sleeper.trino.handle.SleeperSplit;
import sleeper.trino.handle.SleeperTableHandle;
import sleeper.trino.handle.SleeperTransactionHandle;
import sleeper.trino.remotesleeperconnection.SleeperConnectionAsTrino;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Provides a {@link SleeperRecordSet} which will scan an entire {@link SleeperSplit} and return the records in that
 * split.
 */
public class SleeperRecordSetProvider implements ConnectorRecordSetProvider {
    private final SleeperConnectionAsTrino sleeperConnectionAsTrino;

    @Inject
    public SleeperRecordSetProvider(SleeperConnectionAsTrino sleeperConnectionAsTrino) {
        this.sleeperConnectionAsTrino = requireNonNull(sleeperConnectionAsTrino);
    }

    /**
     * Provide a record set according to the supplied parameters.
     *
     * @param transactionHandle          The transaction that the record set is to run under.
     * @param session                    The session that the record set is to run under.
     * @param split                      The split that the record set is to read. The split contains tne details of the
     *                                   Sleeper partition, and the rowkey ranges within that partition, that are to be
     *                                   read.
     * @param tableHandle                The table that the record set is to read. Note that the tupledomain returned by
     *                                   {@link SleeperTableHandle#getTupleDomain()} is ignored and the ranges retrieved
     *                                   from the split are used instead.
     * @param outputColumnHandlesInOrder The column handles to be returned by the record set.
     * @return The record set which corresponds to the supplied parameters.
     */
    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
                                  ConnectorSession session,
                                  ConnectorSplit split,
                                  ConnectorTableHandle tableHandle,
                                  List<? extends ColumnHandle> outputColumnHandlesInOrder) {
        List<SleeperColumnHandle> sleeperColumnHandles = outputColumnHandlesInOrder.stream()
                .map(SleeperColumnHandle.class::cast)
                .collect(ImmutableList.toImmutableList());

        return new SleeperRecordSet(
                sleeperConnectionAsTrino,
                (SleeperTransactionHandle) transactionHandle,
                (SleeperSplit) split,
                sleeperColumnHandles);
    }
}
