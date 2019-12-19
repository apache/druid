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
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;

import java.util.List;
import java.util.function.Consumer;

/**
 * <pre>
 * Build an {@link HandlingInputRowIterator} for {@link IndexTask}s used for range partitioning. Each {@link
 * InputRow} is processed by the following handlers, in order:
 *
 *   1. Null row: If {@link InputRow} is null, invoke the null row {@link Runnable} callback.
 *
 *   2. Invalid timestamp: If {@link InputRow} has an invalid timestamp, throw a {@link ParseException}.
 *
 *   3. Absent bucket interval: If {@link InputRow} has a timestamp that does not match the
 *      {@link GranularitySpec} bucket intervals, invoke the absent bucket interval {@link Consumer}
 *      callback.
 *
 *   4. Filter for rows with only a single dimension value count for the specified partition dimension.
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
   * @param partitionDimension Create range partitions for this dimension
   * @param skipNull Whether to skip rows with a dimension value of null
   */
  public RangePartitionIndexTaskInputRowIteratorBuilder(String partitionDimension, boolean skipNull)
  {
    delegate = new DefaultIndexTaskInputRowIteratorBuilder();

    if (skipNull) {
      delegate.appendInputRowHandler(createOnlySingleDimensionValueRowsHandler(partitionDimension));
    } else {
      delegate.appendInputRowHandler(createOnlySingleOrNullDimensionValueRowsHandler(partitionDimension));
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
  public IndexTaskInputRowIteratorBuilder nullRowRunnable(Runnable nullRowRunnable)
  {
    return delegate.nullRowRunnable(nullRowRunnable);
  }

  @Override
  public IndexTaskInputRowIteratorBuilder absentBucketIntervalConsumer(Consumer<InputRow> absentBucketIntervalConsumer)
  {
    return delegate.absentBucketIntervalConsumer(absentBucketIntervalConsumer);
  }

  @Override
  public HandlingInputRowIterator build()
  {
    return delegate.build();
  }

  private static HandlingInputRowIterator.InputRowHandler createOnlySingleDimensionValueRowsHandler(
      String partitionDimension
  )
  {
    return inputRow -> {
      int dimensionValueCount = getSingleOrNullDimensionValueCount(inputRow, partitionDimension);
      return dimensionValueCount != 1;
    };
  }

  private static HandlingInputRowIterator.InputRowHandler createOnlySingleOrNullDimensionValueRowsHandler(
      String partitionDimension
  )
  {
    return inputRow -> {
      int dimensionValueCount = getSingleOrNullDimensionValueCount(inputRow, partitionDimension);
      return dimensionValueCount > 1;  // Rows.objectToStrings() returns an empty list for a single null value
    };
  }

  private static int getSingleOrNullDimensionValueCount(InputRow inputRow, String partitionDimension)
  {
    List<String> dimensionValues = inputRow.getDimension(partitionDimension);
    int dimensionValueCount = dimensionValues.size();
    if (dimensionValueCount > 1) {
      throw new IAE(
          "Cannot partition on multi-value dimension [%s] for input row [%s]",
          partitionDimension,
          inputRow
      );
    }
    return dimensionValueCount;
  }
}
