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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.std.BasicStandardStageProcessor;
import org.apache.druid.msq.exec.std.ProcessorsAndChannels;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

@JsonTypeName("window")
public class WindowOperatorQueryStageProcessor extends BasicStandardStageProcessor
{
  private final WindowOperatorQuery query;
  private final List<OperatorFactory> operatorList;
  private final RowSignature stageRowSignature;
  private final int maxRowsMaterializedInWindow;
  private final List<String> partitionColumnNames;

  @JsonCreator
  public WindowOperatorQueryStageProcessor(
      @JsonProperty("query") WindowOperatorQuery query,
      @JsonProperty("operatorList") List<OperatorFactory> operatorFactoryList,
      @JsonProperty("stageRowSignature") RowSignature stageRowSignature,
      @Deprecated @JsonProperty("maxRowsMaterializedInWindow") int maxRowsMaterializedInWindow,
      @Deprecated @JsonProperty("partitionColumnNames") List<String> partitionColumnNames
  )
  {
    this.query = Preconditions.checkNotNull(query, "query");
    this.operatorList = Preconditions.checkNotNull(operatorFactoryList, "bad operator");
    this.stageRowSignature = Preconditions.checkNotNull(stageRowSignature, "stageSignature");

    this.maxRowsMaterializedInWindow = maxRowsMaterializedInWindow;

    if (partitionColumnNames == null) {
      throw DruidException.defensive("List of partition column names encountered as null.");
    }
    this.partitionColumnNames = partitionColumnNames;
  }

  @JsonProperty("query")
  public WindowOperatorQuery getQuery()
  {
    return query;
  }

  @JsonProperty("operatorList")
  public List<OperatorFactory> getOperators()
  {
    return operatorList;
  }

  @JsonProperty("stageRowSignature")
  public RowSignature getSignature()
  {
    return stageRowSignature;
  }

  @JsonProperty("partitionColumnNames")
  public List<String> getPartitionColumnNames()
  {
    return partitionColumnNames;
  }

  @JsonProperty("maxRowsMaterializedInWindow")
  public int getMaxRowsMaterializedInWindow()
  {
    return maxRowsMaterializedInWindow;
  }

  @Override
  public ProcessorsAndChannels<Object, Long> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable Object extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher,
      final boolean removeNullBytes
  )
  {
    // Expecting a single input slice from some prior stage.
    final StageInputSlice slice = (StageInputSlice) Iterables.getOnlyElement(inputSlices);
    final Int2ObjectSortedMap<OutputChannel> outputChannels = new Int2ObjectAVLTreeMap<>();

    for (final ReadablePartition partition : slice.getPartitions()) {
      outputChannels.computeIfAbsent(
          partition.getPartitionNumber(),
          i -> {
            try {
              return outputChannelFactory.openChannel(i);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    final Sequence<ReadableInput> readableInputs =
        Sequences.simple(inputSliceReader.attach(0, slice, counters, warningPublisher));

    final Sequence<FrameProcessor<Object>> processors = readableInputs.map(
        readableInput -> {
          final OutputChannel outputChannel =
              outputChannels.get(readableInput.getStagePartition().getPartitionNumber());

          return new WindowOperatorQueryFrameProcessor(
              query.context(),
              readableInput.getChannel(),
              outputChannel.getWritableChannel(),
              stageDefinition.createFrameWriterFactory(outputChannel.getFrameMemoryAllocator(), removeNullBytes),
              readableInput.getChannelFrameReader(),
              frameContext.jsonMapper(),
              operatorList
          );
        }
    );

    return new ProcessorsAndChannels<>(
        ProcessorManagers.of(processors),
        OutputChannels.wrapReadOnly(ImmutableList.copyOf(outputChannels.values()))
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
    WindowOperatorQueryStageProcessor that = (WindowOperatorQueryStageProcessor) o;
    return maxRowsMaterializedInWindow == that.maxRowsMaterializedInWindow
           && Objects.equals(query, that.query)
           && Objects.equals(operatorList, that.operatorList)
           && Objects.equals(stageRowSignature, that.stageRowSignature)
           && Objects.equals(partitionColumnNames, that.partitionColumnNames);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, operatorList, stageRowSignature, maxRowsMaterializedInWindow, partitionColumnNames);
  }
}
