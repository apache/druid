package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.DefaultGroupPartitioner;
import org.apache.druid.query.rowsandcols.GroupPartitioner;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;

import java.util.List;
import java.util.function.Function;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public abstract class WindowRankingProcessorBase implements Processor
{
  private final List<String> groupingCols;
  private final String outputColumn;

  public WindowRankingProcessorBase(
      List<String> groupingCols,
      String outputColumn
  ) {
    this.groupingCols = groupingCols;
    this.outputColumn = outputColumn;
  }

  public RowsAndColumns processInternal(RowsAndColumns incomingPartition, Function<int[], Column> fn)
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);

    GroupPartitioner groupPartitioner = incomingPartition.as(GroupPartitioner.class);
    if (groupPartitioner == null) {
      groupPartitioner = new DefaultGroupPartitioner(incomingPartition);
    }

    return retVal.addColumn(outputColumn, fn.apply(groupPartitioner.computeGroupings(groupingCols)));
  }
}
