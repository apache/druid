package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.StartAndEnd;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;

import java.util.Arrays;
import java.util.List;

/**
 * This Processor assumes that data has already been sorted for it.  It does not re-sort the data and if it is given
 * data that is not in the correct sorted order, its operation is undefined.
 */
public class WindowRankProcessor extends WindowRankingProcessorBase
{
  private final boolean asPercent;

  @JsonCreator
  public WindowRankProcessor(
      @JsonProperty("group") List<String> groupingCols,
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("asPercent") boolean asPercent
  )
  {
    super(groupingCols, outputColumn);
    this.asPercent = asPercent;
  }

  @JsonProperty("asPercent")
  public boolean isAsPercent()
  {
    return asPercent;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    if (asPercent) {
      return processInternal(incomingPartition, groupings -> {
        final double[] percentages = new double[incomingPartition.numRows()];
        final double denominator = percentages.length - 1;

        for (final StartAndEnd startAndEnd : groupings) {
          Arrays.fill(percentages, startAndEnd.getStart(), startAndEnd.getEnd(), startAndEnd.getStart() / denominator);
        }

        return new DoubleArrayColumn(percentages);
      });
    }

    return processInternal(incomingPartition, groupings -> {
      final int[] ranks = new int[incomingPartition.numRows()];

      for (final StartAndEnd startAndEnd : groupings) {
        Arrays.fill(ranks, startAndEnd.getStart(), startAndEnd.getEnd(), startAndEnd.getStart() + 1);
      }

      return new IntArrayColumn(ranks);
    });
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowRankProcessor) {
      WindowRankProcessor other = (WindowRankProcessor) otherProcessor;
      return asPercent == other.asPercent && intervalValidation(other);
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "WindowRankProcessor{" +
           internalToString() +
           ", asPercent=" + asPercent +
           '}';
  }
}
