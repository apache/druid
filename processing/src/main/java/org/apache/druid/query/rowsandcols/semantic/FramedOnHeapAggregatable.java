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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import javax.annotation.Nonnull;

/**
 * A semantic interface used to aggregate a list of AggregatorFactories across a given set of data
 * <p>
 * The aggregation specifically happens on-heap and should be used in places where it is known that the data
 * set can be worked with entirely on-heap.  There is support for frame definitions, frames aggregate certain
 * subsets of rows in a rolling fashion like a windowed average.  Frames are defined in terms of boundaries
 * where a boundary could be based on rows or it could be based on "PEER" groupings.
 * <p>
 * A peer grouping is defined as a set of rows that are the same based on the ORDER BY columns specified.  As such
 * peer-grouped values must also come with a set of ORDER BY columns.
 */
public interface FramedOnHeapAggregatable
{
  static FramedOnHeapAggregatable fromRAC(RowsAndColumns rac)
  {
    FramedOnHeapAggregatable retVal = rac.as(FramedOnHeapAggregatable.class);
    if (retVal == null) {
      retVal = new DefaultFramedOnHeapAggregatable(RowsAndColumns.expectAppendable(rac));
    }
    return retVal;
  }

  /**
   * Aggregates the data according to the {@link WindowFrame} using the {@code AggregatorFactory} objects provided.
   *
   * @param frame        window frame definition
   * @param aggFactories definition of aggregations to be done
   * @return a RowsAndColumns that contains columns representing the results of the aggregation
   * from the AggregatorFactories
   */
  @Nonnull
  RowsAndColumns aggregateAll(WindowFrame frame, AggregatorFactory[] aggFactories);
}
