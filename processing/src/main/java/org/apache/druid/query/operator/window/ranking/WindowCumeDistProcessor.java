package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.StartAndEnd;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;

import java.util.Arrays;
import java.util.List;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public class WindowCumeDistProcessor extends WindowRankingProcessorBase
{
  @JsonCreator
  public WindowCumeDistProcessor(
      @JsonProperty("group") List<String> groupingCols,
      @JsonProperty("outputColumn") String outputColumn
  ) {
    super(groupingCols, outputColumn);
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    return processInternal(incomingPartition, groupings -> {
      final double[] ranks = new double[incomingPartition.numRows()];
      for (final StartAndEnd startAndEnd : groupings) {
        double relativeRank = startAndEnd.getEnd() / (double) ranks.length;
        Arrays.fill(ranks, startAndEnd.getStart(), startAndEnd.getEnd(), relativeRank);
      }

      return new DoubleArrayColumn(ranks);
    });
  }
}
