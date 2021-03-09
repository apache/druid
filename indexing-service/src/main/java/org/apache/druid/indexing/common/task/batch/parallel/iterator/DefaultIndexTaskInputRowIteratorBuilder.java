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

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.HandlingInputRowIterator;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * Build a default {@link HandlingInputRowIterator} for {@link IndexTask}s. Each {@link InputRow} is
 * processed by the registered handlers in the order that they are registered by calls to
 * {@link #appendInputRowHandler(HandlingInputRowIterator.InputRowHandler)}.
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
  public HandlingInputRowIterator build()
  {
    Preconditions.checkNotNull(delegate, "delegate required");
    Preconditions.checkNotNull(granularitySpec, "granularitySpec required");

    ImmutableList.Builder<HandlingInputRowIterator.InputRowHandler> handlersBuilder = ImmutableList.<HandlingInputRowIterator.InputRowHandler>builder()
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
}
