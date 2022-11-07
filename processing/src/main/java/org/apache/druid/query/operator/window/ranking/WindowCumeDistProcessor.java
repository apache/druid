package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;

import java.util.Arrays;
import java.util.List;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public class WindowCumeDistProcessor extends WindowRankingProcessorBase
{
  public WindowCumeDistProcessor(
      List<String> groupingCols,
      String outputColumn
  ) {
    super(groupingCols, outputColumn);
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    return processInternal(incomingPartition, groupings -> {
      final double[] ranks = new double[groupings.length];
      int unsetPoint = 0;
      for (int i = 1; i < groupings.length; ++i) {
        if (groupings[i - 1] != groupings[i]) {
          // The logic here looks like we should be using i-1, but i is zero-indexed and the rank is 1-indexed,
          // so i is actually correct.
          double relativeRank = i / (double) groupings.length;
          Arrays.fill(ranks, unsetPoint, i, relativeRank);
          unsetPoint = i;
        }
      }
      // The last value wasn't filled in, so fill it in with 1.0
      Arrays.fill(ranks, unsetPoint, ranks.length, 1.0D);

      return new DoubleArrayColumn(ranks);
    });
  }
}
