package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.DefaultSortedGroupPartitioner;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.SortedGroupPartitioner;
import org.apache.druid.query.rowsandcols.StartAndEnd;
import org.apache.druid.query.rowsandcols.column.Column;

import java.util.ArrayList;
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
  )
  {
    this.groupingCols = groupingCols;
    this.outputColumn = outputColumn;
  }

  @JsonProperty("group")
  public List<String> getGroupingCols()
  {
    return groupingCols;
  }

  @JsonProperty("outputColumn")
  public String getOutputColumn()
  {
    return outputColumn;
  }

  public RowsAndColumns processInternal(
      RowsAndColumns incomingPartition,
      Function<ArrayList<StartAndEnd>, Column> fn
  )
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);

    SortedGroupPartitioner groupPartitioner = incomingPartition.as(SortedGroupPartitioner.class);
    if (groupPartitioner == null) {
      groupPartitioner = new DefaultSortedGroupPartitioner(incomingPartition);
    }

    retVal.addColumn(outputColumn, fn.apply(groupPartitioner.computeBoundaries(groupingCols)));
    return retVal;
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    return getClass() == otherProcessor.getClass()
           && intervalValidation((WindowRankingProcessorBase) otherProcessor);
  }

  protected boolean intervalValidation(WindowRankingProcessorBase other)
  {
    // Only input needs to be the same for the processors to produce equivalent results
    return groupingCols.equals(other.groupingCols);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" + internalToString() + '}';
  }

  protected String internalToString()
  {
    return "groupingCols=" + groupingCols +
           ", outputColumn='" + outputColumn + '\'';
  }
}
