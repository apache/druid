package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;

import java.util.Arrays;

public class WindowPercentileProcessor implements Processor
{
  private final int numBuckets;
  private final String outputColumn;

  @JsonCreator
  public WindowPercentileProcessor(
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("numBuckets") int numBuckets
  )
  {
    Preconditions.checkArgument(numBuckets > 0, "numBuckets[%s] must be greater than zero", numBuckets);

    this.outputColumn = outputColumn;
    this.numBuckets = numBuckets;
  }

  @JsonProperty("numBuckets")
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty("outputColumn")
  public String getOutputColumn()
  {
    return outputColumn;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);

    int numRows = incomingPartition.numRows();
    int countPerBucket = numRows / numBuckets;
    int extraRows = numRows % numBuckets;

    int index = 0;
    int[] bucketVals = new int[numRows];
    for (int i = 0; i < numBuckets; ++i) {
      int nextIndex = index + countPerBucket;
      if (extraRows > 0) {
        ++nextIndex;
        --extraRows;
      }

      // Buckets are 1-indexed, so we fill with i+1
      Arrays.fill(bucketVals, index, nextIndex, i + 1);

      index = nextIndex;
    }

    return retVal.addColumn(outputColumn, new IntArrayColumn(bucketVals));
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowPercentileProcessor) {
      return numBuckets == ((WindowPercentileProcessor) otherProcessor).numBuckets;
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "WindowPercentileProcessor{" +
           "numBuckets=" + numBuckets +
           ", outputColumn='" + outputColumn + '\'' +
           '}';
  }
}
