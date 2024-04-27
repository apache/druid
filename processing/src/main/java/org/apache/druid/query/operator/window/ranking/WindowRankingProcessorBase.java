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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.DefaultClusteredGroupPartitioner;

import java.util.List;
import java.util.Objects;
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
      Function<int[], Column> fn
  )
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);

    ClusteredGroupPartitioner groupPartitioner = incomingPartition.as(ClusteredGroupPartitioner.class);
    if (groupPartitioner == null) {
      groupPartitioner = new DefaultClusteredGroupPartitioner(incomingPartition);
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

  @Override
  public int hashCode()
  {
    return Objects.hash(groupingCols, outputColumn);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    WindowRankingProcessorBase other = (WindowRankingProcessorBase) obj;
    return Objects.equals(groupingCols, other.groupingCols) && Objects.equals(outputColumn, other.outputColumn);
  }

}
