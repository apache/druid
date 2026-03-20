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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.channel.ReadableConcatFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.std.BasicStageProcessor;
import org.apache.druid.msq.exec.std.ProcessorsAndChannels;
import org.apache.druid.msq.exec.std.StandardStageRunner;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ReadableInput;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;

@JsonTypeName("limit")
public class OffsetLimitStageProcessor extends BasicStageProcessor
{
  private final long offset;

  @Nullable
  private final Long limit;

  @JsonCreator
  public OffsetLimitStageProcessor(
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
  public ListenableFuture<Long> execute(ExecutionContext context)
  {
    final StandardStageRunner<Object, Long> stageRunner = new StandardStageRunner<>(context);

    if (context.workOrder().getWorkerNumber() > 0) {
      // We use a simplistic limiting approach: funnel all data through a single worker, single processor, and
      // single output partition. So limiting stages must have a single worker.
      throw new ISE("%s must be configured with maxWorkerCount = 1", getClass().getSimpleName());
    }

    // Expect a single input slice.
    final StageInputSlice slice = (StageInputSlice) CollectionUtils.getOnlyElement(
        context.workOrder().getInputs(),
        xs -> DruidException.defensive("Expected only a single input slice, but got[%s]", xs)
    );

    if (slice.getPartitions().isEmpty()) {
      return stageRunner.run(
          new ProcessorsAndChannels<>(ProcessorManagers.none(), OutputChannels.none())
      );
    }

    final OutputChannel outputChannel;
    try {
      outputChannel = stageRunner.workOutputChannelFactory().openChannel(0);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    final Supplier<FrameProcessor<Object>> workerSupplier = () -> {
      final Iterable<ReadableInput> readableInputs = Iterables.transform(
          slice.getPartitions(),
          readablePartition -> QueryKitUtils.readPartition(context, readablePartition)
      );

      // Note: OffsetLimitFrameProcessor does not use allocator from the outputChannel; it uses unlimited instead.
      // This ensures that a single, limited output frame can always be generated from an input frame.
      return new OffsetLimitFrameProcessor(
          ReadableConcatFrameChannel.open(Iterators.transform(readableInputs.iterator(), ReadableInput::getChannel)),
          outputChannel.getWritableChannel(),
          context.workOrder().getQueryDefinition().getStageDefinition(slice.getStageNumber()).getFrameReader(),
          context.workOrder().getStageDefinition().createFrameWriterFactory(
              context.frameContext().frameWriterSpec(),
              HeapMemoryAllocator.unlimited()
          ),
          offset,
          // Limit processor will add limit + offset at various points; must avoid overflow
          limit == null ? Long.MAX_VALUE - offset : limit
      );
    };

    return stageRunner.run(
        new ProcessorsAndChannels<>(
            ProcessorManagers.of(workerSupplier),
            OutputChannels.wrap(Collections.singletonList(outputChannel))
        )
    );
  }

  @Override
  public boolean usesProcessingBuffers()
  {
    return false;
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
    OffsetLimitStageProcessor that = (OffsetLimitStageProcessor) o;
    return offset == that.offset && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(offset, limit);
  }
}
