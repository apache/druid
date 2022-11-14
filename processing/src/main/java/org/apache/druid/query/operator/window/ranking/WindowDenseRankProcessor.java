package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.StartAndEnd;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;

import java.util.Arrays;
import java.util.List;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public class WindowDenseRankProcessor extends WindowRankingProcessorBase
{
  @JsonCreator
  public WindowDenseRankProcessor(
      @JsonProperty("group") List<String> groupingCols,
      @JsonProperty("outputColumn") String outputColumn
  ) {
    super(groupingCols, outputColumn);
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    return processInternal(incomingPartition, groupings -> {
      final int[] ranks = new int[incomingPartition.numRows()];
      for (int i = 0; i < groupings.size(); ++i) {
        StartAndEnd startAndEnd = groupings.get(i);
        Arrays.fill(ranks, startAndEnd.getStart(), startAndEnd.getEnd(), i + 1);
      }

      return new IntArrayColumn(ranks);
    });
  }
}
