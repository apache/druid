package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;

import java.util.List;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public class WindowDenseRankProcessor extends WindowRankingProcessorBase
{
  public WindowDenseRankProcessor(
      List<String> groupingCols,
      String outputColumn
  ) {
    super(groupingCols, outputColumn);
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    return processInternal(incomingPartition, groupings -> {
      final int[] ranks = new int[groupings.length];
      ranks[0] = 1;
      for (int i = 1; i < groupings.length; ++i) {
        ranks[i] = ranks[i - 1];
        if (groupings[i - 1] != groupings[i]) {
          ++ranks[i];
        }
      }

      return new IntArrayColumn(ranks);
    });
  }
}
