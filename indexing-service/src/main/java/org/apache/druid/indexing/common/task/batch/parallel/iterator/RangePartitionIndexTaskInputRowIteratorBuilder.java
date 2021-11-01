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

package org.apache.druid.indexing.common.task.batch.parallel.iterator;

import org.apache.druid.data.input.HandlingInputRowIterator;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;

import java.util.List;
import java.util.function.Predicate;

/**
 * <pre>
 * Build an {@link HandlingInputRowIterator} for {@link IndexTask}s used for range partitioning. Each {@link
 * InputRow} is processed by the following handlers, in order:
 *
 *   1. Filter for rows with only a single dimension value count for the specified partition dimension.
 *
 * If any of the handlers invoke their respective callback, the {@link HandlingInputRowIterator} will yield
 * a null {@link InputRow} next; otherwise, the next {@link InputRow} is yielded.
 * </pre>
 *
 * @see DefaultIndexTaskInputRowIteratorBuilder
 */
public class RangePartitionIndexTaskInputRowIteratorBuilder implements IndexTaskInputRowIteratorBuilder
{
  private final DefaultIndexTaskInputRowIteratorBuilder delegate;

  /**
   * @param partitionDimensions Create range partitions for these dimensions
   * @param skipNull Whether to skip rows with a dimension value of null
   */
  public RangePartitionIndexTaskInputRowIteratorBuilder(List<String> partitionDimensions, boolean skipNull)
  {
    delegate = new DefaultIndexTaskInputRowIteratorBuilder();

    if (skipNull) {
      delegate.appendInputRowHandler(createOnlySingleDimensionValueRowsHandler(partitionDimensions));
    } else {
      delegate.appendInputRowHandler(createOnlySingleOrNullDimensionValueRowsHandler(partitionDimensions));
    }
  }

  @Override
  public IndexTaskInputRowIteratorBuilder delegate(CloseableIterator<InputRow> inputRowIterator)
  {
    return delegate.delegate(inputRowIterator);
  }

  @Override
  public IndexTaskInputRowIteratorBuilder granularitySpec(GranularitySpec granularitySpec)
  {
    return delegate.granularitySpec(granularitySpec);
  }

  @Override
  public HandlingInputRowIterator build()
  {
    return delegate.build();
  }

  private static HandlingInputRowIterator.InputRowHandler createOnlySingleDimensionValueRowsHandler(
      List<String> partitionDimensions
  )
  {
    return inputRow -> isRowHandled(inputRow, partitionDimensions, dimValueCount -> dimValueCount != 1);
  }

  private static HandlingInputRowIterator.InputRowHandler createOnlySingleOrNullDimensionValueRowsHandler(
      List<String> partitionDimensions
  )
  {
    // Rows.objectToStrings() returns an empty list for a single null value
    return inputRow -> isRowHandled(inputRow, partitionDimensions, dimValueCount -> dimValueCount > 1);
  }

  /**
   * @param valueCountPredicate Predicate that must be satisfied
   *                            for atleast one of the partitionDimensions for the row to be marked as handled.
   * @return true when the given InputRow should be marked handled
   * and need not be processed further.
   */
  private static boolean isRowHandled(
      InputRow inputRow,
      List<String> partitionDimensions,
      Predicate<Integer> valueCountPredicate
  )
  {
    for (String dimension : partitionDimensions) {
      int dimensionValueCount = inputRow.getDimension(dimension).size();
      if (dimensionValueCount > 1) {
        throw new IAE(
            "Cannot partition on multi-value dimension [%s] for input row [%s]",
            dimension,
            inputRow
        );
      } else if (valueCountPredicate.test(dimensionValueCount)) {
        return true;
      }
    }

    return false;
  }

}
