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

package org.apache.druid.query.operator.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.DefaultFramedOnHeapAggregatable;
import org.apache.druid.query.rowsandcols.semantic.FramedOnHeapAggregatable;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class WindowFramedAggregateProcessor implements Processor
{
  @Nullable
  private static <T> T[] emptyToNull(T[] arr)
  {
    if (arr == null || arr.length == 0) {
      return null;
    } else {
      return arr;
    }
  }

  private final WindowFrame frame;
  private final AggregatorFactory[] aggregations;

  @JsonCreator
  public WindowFramedAggregateProcessor(
      @JsonProperty("frame") WindowFrame frame,
      @JsonProperty("aggregations") AggregatorFactory[] aggregations
  )
  {
    this.frame = frame;
    this.aggregations = emptyToNull(aggregations);
  }

  @JsonProperty("frame")
  public WindowFrame getFrame()
  {
    return frame;
  }

  @JsonProperty("aggregations")
  public AggregatorFactory[] getAggregations()
  {
    return aggregations;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns inputPartition)
  {
    FramedOnHeapAggregatable agger = inputPartition.as(FramedOnHeapAggregatable.class);
    if (agger == null) {
      agger = new DefaultFramedOnHeapAggregatable(RowsAndColumns.expectAppendable(inputPartition));
    }
    return agger.aggregateAll(frame, aggregations);
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowFramedAggregateProcessor) {
      WindowFramedAggregateProcessor other = (WindowFramedAggregateProcessor) otherProcessor;
      return frame.equals(other.frame) && Arrays.equals(aggregations, other.aggregations);
    }
    return false;
  }


  @Override
  public String toString()
  {
    return "WindowFramedAggregateProcessor{" +
           "frame=" + frame +
           ", aggregations=" + Arrays.toString(aggregations) +
           '}';
  }

  @Override
  public int hashCode()
  {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(aggregations);
    result = prime * result + Objects.hash(frame);
    return result;
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
    WindowFramedAggregateProcessor other = (WindowFramedAggregateProcessor) obj;
    return Arrays.equals(aggregations, other.aggregations) && Objects.equals(frame, other.frame);
  }


}
