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
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
        if (percentages.length > 1) {
          final double denominator = percentages.length - 1;

          for (int i = 1; i < groupings.length; ++i) {
            final int start = groupings[i - 1];
            final int end = groupings[i];
            Arrays.fill(percentages, start, end, start / denominator);
          }
        }

        return new DoubleArrayColumn(percentages);
      });
    }

    return processInternal(incomingPartition, groupings -> {
      final int[] ranks = new int[incomingPartition.numRows()];

      for (int i = 1; i < groupings.length; ++i) {
        final int start = groupings[i - 1];
        final int end = groupings[i];
        Arrays.fill(ranks, start, end, start + 1);
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

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Objects.hash(asPercent);
    return result;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    WindowRankProcessor other = (WindowRankProcessor) obj;
    return asPercent == other.asPercent;
  }
}
