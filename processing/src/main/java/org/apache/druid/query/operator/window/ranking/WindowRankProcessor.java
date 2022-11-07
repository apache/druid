package org.apache.druid.query.operator.window.ranking;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;

import java.util.List;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public class WindowRankProcessor extends WindowRankingProcessorBase
{
  private final boolean asPercent;

  public WindowRankProcessor(
      List<String> groupingCols,
      String outputColumn,
      boolean asPercent
  ) {
    super(groupingCols, outputColumn);
    this.asPercent = asPercent;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    return processInternal(incomingPartition, groupings -> {
      final int[] ranks = new int[groupings.length];
      ranks[0] = 1;
      for (int i = 1; i < groupings.length; ++i) {
        if (groupings[i - 1] == groupings[i]) {
          ranks[i] = ranks[i - 1];
        } else {
          // ranks are 1-indexed
          ranks[i] = i + 1;
        }
      }

      if (asPercent) {
        final double[] percentages = new double[ranks.length];
        final double denominator = ranks.length - 1;

        for (int i = 0; i < ranks.length; i++) {
          percentages[i] = (ranks[i] - 1) / denominator;
        }

        return new DoubleArrayColumn(percentages);
      }
      return new IntArrayColumn(ranks);
    });
  }
}
