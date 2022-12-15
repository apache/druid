/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;

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

    retVal.addColumn(outputColumn, new IntArrayColumn(bucketVals));
    return retVal;
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
