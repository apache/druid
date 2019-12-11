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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.HandlingInputRowIterator;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * <pre>
 * Build a default {@link HandlingInputRowIterator} for {@link IndexTask}s. Each {@link InputRow} is
 * processed by the following handlers, in order:
 *
 *   1. Null row: If {@link InputRow} is null, invoke the null row {@link Runnable} callback.
 *
 *   2. Invalid timestamp: If {@link InputRow} has an invalid timestamp, throw a {@link ParseException}.
 *
 *   3. Absent bucket interval: If {@link InputRow} has a timestamp that does not match the
 *      {@link GranularitySpec} bucket intervals, invoke the absent bucket interval {@link Consumer}
 *      callback.
 *
 *   4. Any additional handlers in the order they are added by calls to
 *      {@link #appendInputRowHandler(HandlingInputRowIterator.InputRowHandler)}.
 *
 * If any of the handlers invoke their respective callback, the {@link HandlingInputRowIterator} will yield
 * a null {@link InputRow} next; otherwise, the next {@link InputRow} is yielded.
 * </pre>
 *
 * @see RangePartitionIndexTaskInputRowIteratorBuilder
 */
public class DefaultIndexTaskInputRowIteratorBuilder implements IndexTaskInputRowIteratorBuilder
{
  private CloseableIterator<InputRow> delegate = null;
  private GranularitySpec granularitySpec = null;
  private HandlingInputRowIterator.InputRowHandler nullRowHandler = null;
  private HandlingInputRowIterator.InputRowHandler absentBucketIntervalHandler = null;
  private final List<HandlingInputRowIterator.InputRowHandler> appendedInputRowHandlers = new ArrayList<>();

  @Override
  public DefaultIndexTaskInputRowIteratorBuilder delegate(CloseableIterator<InputRow> inputRowIterator)
  {
    this.delegate = inputRowIterator;
    return this;
  }

  @Override
  public DefaultIndexTaskInputRowIteratorBuilder granularitySpec(GranularitySpec granularitySpec)
  {
    this.granularitySpec = granularitySpec;
    return this;
  }

  @Override
  public DefaultIndexTaskInputRowIteratorBuilder nullRowRunnable(Runnable nullRowRunnable)
  {
    this.nullRowHandler = inputRow -> {
      if (inputRow == null) {
        nullRowRunnable.run();
        return true;
      }
      return false;
    };
    return this;
  }

  @Override
  public DefaultIndexTaskInputRowIteratorBuilder absentBucketIntervalConsumer(
      Consumer<InputRow> absentBucketIntervalConsumer
  )
  {
    this.absentBucketIntervalHandler = inputRow -> {
      Optional<Interval> intervalOpt = granularitySpec.bucketInterval(inputRow.getTimestamp());
      if (!intervalOpt.isPresent()) {
        absentBucketIntervalConsumer.accept(inputRow);
        return true;
      }
      return false;
    };
    return this;
  }

  @Override
  public HandlingInputRowIterator build()
  {
    Preconditions.checkNotNull(delegate, "delegate required");
    Preconditions.checkNotNull(granularitySpec, "granularitySpec required");
    Preconditions.checkNotNull(nullRowHandler, "nullRowRunnable required");
    Preconditions.checkNotNull(absentBucketIntervalHandler, "absentBucketIntervalConsumer required");

    ImmutableList.Builder<HandlingInputRowIterator.InputRowHandler> handlersBuilder = ImmutableList.<HandlingInputRowIterator.InputRowHandler>builder()
        .add(nullRowHandler)
        .add(createInvalidTimestampHandler())
        .add(absentBucketIntervalHandler)
        .addAll(appendedInputRowHandlers);

    return new HandlingInputRowIterator(delegate, handlersBuilder.build());
  }

  /**
   * @param inputRowHandler Optionally, append this input row handler to the required ones.
   */
  DefaultIndexTaskInputRowIteratorBuilder appendInputRowHandler(HandlingInputRowIterator.InputRowHandler inputRowHandler)
  {
    this.appendedInputRowHandlers.add(inputRowHandler);
    return this;
  }

  private HandlingInputRowIterator.InputRowHandler createInvalidTimestampHandler()
  {
    return inputRow -> {
      if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
        String errorMsg = StringUtils.format(
            "Encountered row with timestamp that cannot be represented as a long: [%s]",
            inputRow
        );
        throw new ParseException(errorMsg);
      }
      return false;
    };
  }
}
