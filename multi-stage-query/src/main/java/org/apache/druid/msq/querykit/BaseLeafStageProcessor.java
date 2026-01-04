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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableConcatFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.std.BasicStageProcessor;
import org.apache.druid.msq.exec.std.ProcessorsAndChannels;
import org.apache.druid.msq.exec.std.StandardPartitionReader;
import org.apache.druid.msq.exec.std.StandardStageRunner;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.external.ExternalInputSlice;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

/**
 * Base class of frame processors that can read regular Druid segments, external data, *or* channels from
 * other stages. The term "leaf" represents the fact that they are capable of being leaves in the query tree. However,
 * they do not *need* to be leaves. They can read from prior stages as well.
 */
public abstract class BaseLeafStageProcessor extends BasicStageProcessor
{
  private final Query<?> query;

  protected BaseLeafStageProcessor(Query<?> query)
  {
    this.query = query;
  }

  @Override
  public ListenableFuture<Long> execute(ExecutionContext context)
  {
    final StandardStageRunner<Object, Long> stageRunner = new StandardStageRunner<>(context);
    final List<InputSlice> inputSlices = context.workOrder().getInputs();
    final StageDefinition stageDefinition = context.workOrder().getStageDefinition();
    final FrameContext frameContext = context.frameContext();

    // BaseLeafStageProcessor is used for native Druid queries, where the following input cases can happen:
    //   1) Union datasources: N nonbroadcast inputs, which are treated as one big input
    //   2) Join datasources: one nonbroadcast input, N broadcast inputs
    //   3) All other datasources: single input

    // No need to close this until startLoadaheadIfNeeded() is called.
    final ReadableInputQueue baseInputQueue = makeBaseInputQueue(context.workOrder().getInputs(), context);
    final int totalProcessors = baseInputQueue.remaining();

    if (totalProcessors == 0) {
      return stageRunner.run(new ProcessorsAndChannels<>(ProcessorManagers.none(), OutputChannels.none()));
    }

    final int outstandingProcessors;

    if (hasParquet(inputSlices)) {
      // This is a workaround for memory use in ParquetFileReader, which loads up an entire row group into memory as
      // part of its normal operation. Row groups can be quite large (like, 1GB large) so this is a major source of
      // unaccounted-for memory use during ingestion and query of external data. We are trying to prevent memory
      // overload by running only a single processor at once.
      outstandingProcessors = 1;
    } else {
      outstandingProcessors = Math.min(totalProcessors, context.threadCount());
    }

    final Queue<FrameWriterFactory> frameWriterFactoryQueue = new ArrayDeque<>(outstandingProcessors);
    final Queue<WritableFrameChannel> channelQueue = new ArrayDeque<>(outstandingProcessors);
    final List<OutputChannel> outputChannels = new ArrayList<>(outstandingProcessors);

    for (int i = 0; i < outstandingProcessors; i++) {
      final OutputChannel outputChannel;
      try {
        outputChannel = stageRunner.workOutputChannelFactory().openChannel(0 /* Partition number doesn't matter */);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      outputChannels.add(outputChannel);
      channelQueue.add(outputChannel.getWritableChannel());
      frameWriterFactoryQueue.add(
          stageDefinition.createFrameWriterFactory(
              frameContext.frameWriterSpec(),
              outputChannel.getFrameMemoryAllocator()
          )
      );
    }

    // SegmentMapFn processor, if needed. May be null.
    final FrameProcessor<SegmentMapFunction> segmentMapFnProcessor = makeSegmentMapFnProcessor(context);

    // Function to generate a processor manger for the regular processors, which run after the segmentMapFnProcessor.
    final Function<List<SegmentMapFunction>, ProcessorManager<Object, Long>> processorManagerFn = segmentMapFnList -> {
      final SegmentMapFunction segmentMapFunction = CollectionUtils.getOnlyElement(
          segmentMapFnList,
          fns -> DruidException.defensive("Only one segment map function expected, got[%s]", fns)
      );
      return createBaseLeafProcessorManagerWithHandoff(
          context,
          baseInputQueue,
          segmentMapFunction,
          frameWriterFactoryQueue,
          channelQueue
      );
    };

    //noinspection rawtypes
    final ProcessorManager processorManager;

    if (segmentMapFnProcessor == null) {
      final SegmentMapFunction segmentMapFn = ExecutionVertex.of(query).createSegmentMapFunction(frameContext.policyEnforcer());
      processorManager = processorManagerFn.apply(ImmutableList.of(segmentMapFn));
    } else {
      processorManager = new ChainedProcessorManager<>(
          ProcessorManagers.of(() -> segmentMapFnProcessor),
          processorManagerFn
      );
    }

    //noinspection rawtypes,unchecked
    return stageRunner.run(
        new ProcessorsAndChannels<>(
            processorManager,
            OutputChannels.wrap(outputChannels)
        )
    );
  }

  private ProcessorManager<Object, Long> createBaseLeafProcessorManagerWithHandoff(
      final ExecutionContext context,
      final ReadableInputQueue baseInputQueue,
      final SegmentMapFunction segmentMapFunction,
      final Queue<FrameWriterFactory> frameWriterFactoryQueue,
      final Queue<WritableFrameChannel> channelQueue
  )
  {
    final BaseLeafStageProcessor factory = this;

    return new ChainedProcessorManager<>(
        new BaseLeafFrameProcessorManager(
            baseInputQueue,
            segmentMapFunction,
            frameWriterFactoryQueue,
            channelQueue,
            context.frameContext(),
            factory
        ),
        objects -> {
          if (objects == null || objects.isEmpty()) {
            return ProcessorManagers.none();
          }
          List<InputSlice> handedOffSegments = new ArrayList<>();
          for (Object o : objects) {
            if (o instanceof SegmentsInputSlice) {
              SegmentsInputSlice slice = (SegmentsInputSlice) o;
              handedOffSegments.add(slice);
            }
          }

          // Fetch any handed off segments from deep storage and try again.
          return new BaseLeafFrameProcessorManager(
              makeBaseInputQueue(handedOffSegments, context),
              segmentMapFunction,
              frameWriterFactoryQueue,
              channelQueue,
              context.frameContext(),
              factory
          );
        }
    );
  }

  protected abstract FrameProcessor<Object> makeProcessor(
      ReadableInput baseInput,
      SegmentMapFunction segmentMapFn,
      ResourceHolder<WritableFrameChannel> outputChannelHolder,
      ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      FrameContext providerThingy
  );

  /**
   * Read base inputs, where "base" is meant in the same sense as in {@link ExecutionVertex}: the primary datasource
   * that drives query processing.
   *
   * The returned {@link ReadableInputQueue} does not need to be closed, because it has not started loading any
   * segments. Once {@link ReadableInputQueue#nextInput()} or {@link ReadableInputQueue#startLoadaheadIfNeeded()} is called,
   * the queue must be closed when done being used.
   */
  private static ReadableInputQueue makeBaseInputQueue(
      final List<InputSlice> inputSlices,
      final ExecutionContext context
  )
  {
    final StageDefinition stageDef = context.workOrder().getStageDefinition();
    final List<PhysicalInputSlice> physicalInputSlices = new ArrayList<>();

    for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
      if (!stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
        physicalInputSlices.add(
            context.inputSliceReader().attach(
                inputNumber,
                inputSlices.get(inputNumber),
                context.counters(),
                context::onWarning
            )
        );
      }
    }

    final Integer segmentLoadAheadCount =
        MultiStageQueryContext.getSegmentLoadAheadCount(context.workOrder().getWorkerContext());
    return new ReadableInputQueue(
        stageDef.getId().getQueryId(),
        new StandardPartitionReader(context),
        physicalInputSlices,
        segmentLoadAheadCount != null ? segmentLoadAheadCount : context.threadCount()
    );
  }

  /**
   * Reads all broadcast inputs of type {@link StageInputSlice}. Returns a map of input number -> channel containing
   * all data for that input number.
   *
   * Broadcast inputs that are not type {@link StageInputSlice} are ignored.
   */
  private static Int2ObjectMap<ReadableInput> readBroadcastInputsFromEarlierStages(ExecutionContext context)
  {
    final StageDefinition stageDef = context.workOrder().getStageDefinition();
    final List<InputSlice> inputSlices = context.workOrder().getInputs();
    final Int2ObjectMap<ReadableInput> broadcastInputs = new Int2ObjectAVLTreeMap<>();
    final StandardPartitionReader partitionReader = new StandardPartitionReader(context);

    try {
      for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
        if (stageDef.getBroadcastInputNumbers().contains(inputNumber)
            && inputSlices.get(inputNumber) instanceof StageInputSlice) {
          final StageInputSlice slice = (StageInputSlice) inputSlices.get(inputNumber);

          // We know ReadableInput::getChannel is OK, because StageInputSlice always uses channels (never segments).
          final ReadableFrameChannel channel = ReadableConcatFrameChannel.open(
              Iterators.transform(
                  slice.getPartitions().iterator(),
                  partition -> {
                    try {
                      return partitionReader.openChannel(partition);
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              )
          );
          final FrameReader frameReader = partitionReader.frameReader(slice.getStageNumber());
          broadcastInputs.put(inputNumber, ReadableInput.channel(channel, frameReader, null));
        }
      }

      return broadcastInputs;
    }
    catch (Throwable e) {
      // Close any already-opened channels.
      try {
        broadcastInputs.values().forEach(input -> input.getChannel().close());
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }

      throw e;
    }
  }

  /**
   * Creates a processor that builds the segmentMapFn for all other processors. Must be run prior to all other
   * processors being run. Returns null if a dedicated segmentMapFn processor is unnecessary.
   */
  @Nullable
  private FrameProcessor<SegmentMapFunction> makeSegmentMapFnProcessor(ExecutionContext context)
  {
    // Read broadcast data once, so it can be reused across all processors in the form of a segmentMapFn.
    // No explicit cleanup: let the garbage collector handle it.
    final Int2ObjectMap<ReadableInput> broadcastInputs = readBroadcastInputsFromEarlierStages(context);

    if (broadcastInputs.isEmpty()) {
      if (ExecutionVertex.of(query).isSegmentMapFunctionExpensive()) {
        // Joins may require significant computation to compute the segmentMapFn. Offload it to a processor.
        return new SimpleSegmentMapFnProcessor(query, context.frameContext().policyEnforcer());
      } else {
        // Non-joins are expected to have cheap-to-compute segmentMapFn. Do the computation in the factory thread,
        // without offloading to a processor.
        return null;
      }
    } else {
      return BroadcastJoinSegmentMapFnProcessor.create(
          query,
          context.frameContext().policyEnforcer(),
          broadcastInputs,
          context.frameContext().memoryParameters().getBroadcastBufferMemory()
      );
    }
  }

  private static boolean hasParquet(final List<InputSlice> slices)
  {
    return slices.stream().anyMatch(
        slice ->
            slice instanceof ExternalInputSlice
            && ((ExternalInputSlice) slice).getInputFormat().getClass().getName().contains("Parquet")
    );
  }
}
