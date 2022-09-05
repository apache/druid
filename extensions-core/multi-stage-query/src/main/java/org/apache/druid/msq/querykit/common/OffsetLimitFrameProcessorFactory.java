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

package org.apache.druid.msq.querykit.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.druid.frame.channel.ReadableConcatFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.BaseFrameProcessorFactory;
import org.apache.druid.msq.util.SupplierIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

@JsonTypeName("limit")
public class OffsetLimitFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private final long offset;

  @Nullable
  private final Long limit;

  @JsonCreator
  public OffsetLimitFrameProcessorFactory(
      @JsonProperty("offset") final long offset,
      @Nullable @JsonProperty("limit") final Long limit
  )
  {
    this.offset = offset;
    this.limit = limit;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getOffset()
  {
    return offset;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getLimit()
  {
    return limit;
  }

  @Override
  public ProcessorsAndChannels<FrameProcessor<Long>, Long> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  ) throws IOException
  {
    if (workerNumber > 0) {
      // We use a simplistic limiting approach: funnel all data through a single worker, single processor, and
      // single output partition. So limiting stages must have a single worker.
      throw new ISE("%s must be configured with maxWorkerCount = 1", getClass().getSimpleName());
    }

    // Expect a single input slice.
    final InputSlice slice = Iterables.getOnlyElement(inputSlices);

    if (inputSliceReader.numReadableInputs(slice) == 0) {
      return new ProcessorsAndChannels<>(Sequences.empty(), OutputChannels.none());
    }

    final OutputChannel outputChannel = outputChannelFactory.openChannel(0);

    final Supplier<FrameProcessor<Long>> workerSupplier = () -> {
      final ReadableInputs readableInputs = inputSliceReader.attach(0, slice, counters, warningPublisher);

      if (!readableInputs.isChannelBased()) {
        throw new ISE("Processor inputs must be channels");
      }

      // Note: OffsetLimitFrameProcessor does not use allocator from the outputChannel; it uses unlimited instead.
      return new OffsetLimitFrameProcessor(
          ReadableConcatFrameChannel.open(Iterators.transform(readableInputs.iterator(), ReadableInput::getChannel)),
          outputChannel.getWritableChannel(),
          readableInputs.frameReader(),
          offset,
          // Limit processor will add limit + offset at various points; must avoid overflow
          limit == null ? Long.MAX_VALUE - offset : limit
      );
    };

    final Sequence<FrameProcessor<Long>> processors =
        Sequences.simple(() -> new SupplierIterator<>(workerSupplier));

    return new ProcessorsAndChannels<>(
        processors,
        OutputChannels.wrapReadOnly(Collections.singletonList(outputChannel))
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OffsetLimitFrameProcessorFactory that = (OffsetLimitFrameProcessorFactory) o;
    return offset == that.offset && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(offset, limit);
  }
}
