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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.PhysicalInputSlice;
import org.apache.druid.msq.input.external.ExternalInputSlice;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.Query;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.loading.AcquireMode;
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
  /**
   * Originally-supplied query. For backwards compatibility, generally workers should avoid using this, in favor
   * of using the query returned by {@link BaseInputVertex#withNumberedBase(Query, IntList)}.
   */
  private final Query<?> originalQuery;

  /**
   * Vertex for each base input of {@link #originalQuery}, keyed by input number. Memoized, since {@link #getPruner}
   * is called once per input.
   */
  private final Supplier<Int2ObjectMap<ExecutionVertex>> baseInputVerticesSupplier;

  protected BaseLeafStageProcessor(Query<?> originalQuery)
  {
    this.originalQuery = originalQuery;
    this.baseInputVerticesSupplier = Suppliers.memoize(() -> {
      final Int2ObjectMap<ExecutionVertex> vertices = new Int2ObjectOpenHashMap<>();
      for (final BaseInputVertex baseInput : BaseInputVertex.createAll(originalQuery)) {
        vertices.put(baseInput.inputNumber(), baseInput.vertex());
      }
      return vertices;
    });
  }

  @Override
  public ListenableFuture<Long> execute(ExecutionContext context)
  {
    final StandardStageRunner<Object, Long> stageRunner = new StandardStageRunner<>(context);
    configureStageRunner(stageRunner, context);
    final List<InputSlice> inputSlices = context.workOrder().getInputs();
    final StageDefinition stageDefinition = context.workOrder().getStageDefinition();
    final FrameContext frameContext = context.frameContext();

    // BaseLeafStageProcessor is used for native Druid queries, where the following inputs can be seen:
    //   1) 1-N nonbroadcast inputs: base inputs, either from a single leaf datasource, or multiple if there is a union
    //   2) 0-N broadcast inputs: if these exist, they are associated with broadcast joins

    // Base inputs: everything that isn't broadcast.
    final IntList baseInputNumbers = baseInputNumbers(context);
    final List<InputSlice> baseInputSlices = new ArrayList<>(baseInputNumbers.size());
    for (final int inputNumber : baseInputNumbers) {
      baseInputSlices.add(inputSlices.get(inputNumber));
    }

    // No need to close this until startLoadaheadIfNeeded() is called.
    final ReadableInputQueue baseInputQueue = makeBaseInputQueue(baseInputNumbers, baseInputSlices, context);
    final int totalProcessors = baseInputQueue.remaining();

    final int outstandingProcessors;

    if (totalProcessors == 0) {
      // No processors to run, but still acquire 1 slice so processingBouncer() works in stageRunner.run().
      outstandingProcessors = 1;
    } else if (hasParquet(inputSlices)) {
      // This is a workaround for memory use in ParquetFileReader, which loads up an entire row group into memory as
      // part of its normal operation. Row groups can be quite large (like, 1GB large) so this is a major source of
      // unaccounted-for memory use during ingestion and query of external data. We are trying to prevent memory
      // overload by running only a single processor at once.
      outstandingProcessors = 1;
    } else {
      outstandingProcessors = Math.min(totalProcessors, context.threadCount());
    }

    if (usesProcessingBuffers()) {
      frameContext.acquireProcessingBuffers(outstandingProcessors);
    }

    if (totalProcessors == 0) {
      return stageRunner.run(new ProcessorsAndChannels<>(ProcessorManagers.none(), OutputChannels.none()));
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
    final Query<?> query = BaseInputVertex.withNumberedBase(originalQuery, baseInputNumbers);
    final FrameProcessor<List<BaseInputSegmentMapFunction>> segmentMapFnProcessor =
        makeSegmentMapFnProcessor(context, query);

    // Function to generate a processor manger for the regular processors, which run after the segmentMapFnProcessor.
    final Function<List<List<BaseInputSegmentMapFunction>>, ProcessorManager<Object, Long>> processorManagerFn =
        segmentMapFnLists -> {
          final List<BaseInputSegmentMapFunction> segmentMapFns = CollectionUtils.getOnlyElement(
              segmentMapFnLists,
              lists -> DruidException.defensive("Only one set of segment map functions expected, got[%s]", lists)
          );

          return createBaseLeafProcessorManagerWithHandoff(
              context,
              baseInputQueue,
              segmentMapFns,
              frameWriterFactoryQueue,
              channelQueue
          );
        };

    //noinspection rawtypes
    final ProcessorManager processorManager;

    if (segmentMapFnProcessor == null) {
      final List<BaseInputSegmentMapFunction> segmentMapFns =
          BaseInputSegmentMapFunction.createAll(query, frameContext.policyEnforcer());
      processorManager = processorManagerFn.apply(List.of(segmentMapFns));
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

  @Nullable
  @Override
  public SegmentPruner getPruner(InputSpec inputSpec, int inputNumber)
  {
    final ExecutionVertex vertex = baseInputVerticesSupplier.get().get(inputNumber);
    return vertex == null ? null : vertex.getSegmentPruner();
  }

  private ProcessorManager<Object, Long> createBaseLeafProcessorManagerWithHandoff(
      final ExecutionContext context,
      final ReadableInputQueue baseInputQueue,
      final List<BaseInputSegmentMapFunction> segmentMapFns,
      final Queue<FrameWriterFactory> frameWriterFactoryQueue,
      final Queue<WritableFrameChannel> channelQueue
  )
  {
    final BaseLeafStageProcessor factory = this;

    return new ChainedProcessorManager<>(
        new BaseLeafFrameProcessorManager(
            baseInputQueue,
            segmentMapFns,
            frameWriterFactoryQueue,
            channelQueue,
            context.frameContext(),
            factory
        ),
        objects -> {
          if (objects == null || objects.isEmpty()) {
            return ProcessorManagers.none();
          }

          final IntList handedOffInputNumbers = new IntArrayList();
          final List<InputSlice> handedOffSegments = new ArrayList<>();
          for (Object o : objects) {
            if (o instanceof BaseLeafFrameProcessorManager.HandedOffSegments handedOff) {
              handedOffInputNumbers.add(handedOff.inputNumber());
              handedOffSegments.add(handedOff.slice());
            }
          }

          // Fetch any handed off segments from deep storage and try again.
          return new BaseLeafFrameProcessorManager(
              makeBaseInputQueue(handedOffInputNumbers, handedOffSegments, context),
              segmentMapFns,
              frameWriterFactoryQueue,
              channelQueue,
              context.frameContext(),
              factory
          );
        }
    );
  }

  /**
   * Hook for subclasses to configure the stage runner before execution.
   */
  protected void configureStageRunner(
      final StandardStageRunner<Object, Long> stageRunner,
      final ExecutionContext context
  )
  {
    // Default: no-op
  }

  protected abstract FrameProcessor<Object> makeProcessor(
      ReadableInput baseInput,
      SegmentMapFunction segmentMapFn,
      ResourceHolder<WritableFrameChannel> outputChannelHolder,
      ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      FrameContext providerThingy
  );

  /**
   * Filters the physical input slices before they are used to create a {@link ReadableInputQueue}.
   * Subclasses can override this to reduce the set of segments that need to be read.
   */
  protected List<PhysicalInputSlice> filterBaseInput(final List<PhysicalInputSlice> slices)
  {
    return slices;
  }


  /**
   * How the {@link ReadableInputQueue} for this stage's base inputs acquires segments, unless overridden by
   * {@link org.apache.druid.query.context.QueryContextParameters#SEGMENT_ACQUIRE_MODE}. The default acquires them fully up front
   * ({@link AcquireMode#FULL}), which is safe for any processor. Leaf processors that read segments through the async
   * cursor API ({@link org.apache.druid.segment.CursorFactory#makeCursorHolderAsync}) can override to
   * {@link AcquireMode#PARTIAL} so only the columns a query actually touches are downloaded.
   */
  protected AcquireMode getDefaultAcquireMode()
  {
    return AcquireMode.FULL;
  }

  /**
   * Pruner that the {@link ReadableInputQueue} for this stage's base inputs uses to skip inputs that cannot contribute
   * to the stage's output.
   */
  protected InputPruner getInputPruner()
  {
    return InputPruner.NEVER;
  }

  /**
   * Read base inputs, where "base" is meant in the same sense as in {@link ExecutionVertex}: the primary datasource
   * that drives query processing.
   *
   * The returned {@link ReadableInputQueue} does not need to be closed, because it has not started loading any
   * segments. Once {@link ReadableInputQueue#nextInput()} or {@link ReadableInputQueue#start()} is called,
   * the queue must be closed when done being used.
   */
  private ReadableInputQueue makeBaseInputQueue(
      final IntList inputNumbers,
      final List<InputSlice> inputSlices,
      final ExecutionContext context
  )
  {
    final List<PhysicalInputSlice> physicalInputSlices = new ArrayList<>(inputNumbers.size());

    for (int i = 0; i < inputNumbers.size(); i++) {
      physicalInputSlices.add(
          context.inputSliceReader().attach(
              inputNumbers.getInt(i),
              inputSlices.get(i),
              context.counters(),
              context::onWarning
          )
      );
    }

    final List<PhysicalInputSlice> filteredSlices = filterBaseInput(physicalInputSlices);

    if (filteredSlices.size() != physicalInputSlices.size()) {
      throw DruidException.defensive(
          "filterBaseInput returned[%d] slices, expected[%d]; it must retain one slice per base input",
          filteredSlices.size(),
          physicalInputSlices.size()
      );
    }

    return new ReadableInputQueue(
        new StandardPartitionReader(context),
        inputNumbers,
        filteredSlices,
        context.segmentLoadAheadCount(),
        context.workOrder().getWorkerContext().getSegmentAcquireMode(getDefaultAcquireMode()),
        getInputPruner()
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
          broadcastInputs.put(inputNumber, ReadableInput.channel(channel, frameReader, slice.getStageNumber(), ReadableInput.NO_PARTITION));
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
   * Creates a processor that builds the segmentMapFns for all other processors. Must be run prior to all other
   * processors being run. Returns null if a dedicated segmentMapFn processor is unnecessary.
   *
   * @param context       execution context
   * @param numberedQuery the stage's query, from {@link BaseInputVertex#withNumberedBase}
   */
  @Nullable
  private FrameProcessor<List<BaseInputSegmentMapFunction>> makeSegmentMapFnProcessor(
      final ExecutionContext context,
      final Query<?> numberedQuery
  )
  {
    // Read broadcast data once, so it can be reused across all processors in the form of a segmentMapFn.
    // No explicit cleanup: let the garbage collector handle it.
    final Int2ObjectMap<ReadableInput> broadcastInputs = readBroadcastInputsFromEarlierStages(context);

    if (broadcastInputs.isEmpty()) {
      final List<ExecutionVertex> baseVertices = ExecutionVertex.of(numberedQuery).splitUnion();
      if (baseVertices.stream().anyMatch(ExecutionVertex::isSegmentMapFunctionExpensive)) {
        // Expensive-to-produce SegmentMapFunctions are offloaded to their own processor.
        return new SimpleSegmentMapFnProcessor(numberedQuery, context.frameContext().policyEnforcer());
      } else {
        // Cheap-to-produce SegmentMapFunctions are built in the factory thread, without offloading to a processor.
        return null;
      }
    } else {
      return BroadcastJoinSegmentMapFnProcessor.create(
          numberedQuery,
          context.frameContext().policyEnforcer(),
          broadcastInputs,
          context.frameContext().memoryParameters().getBroadcastBufferMemory()
      );
    }
  }

  /**
   * Input numbers of this stage's base inputs, in ascending order. "Base" is meant in the same sense as in
   * {@link ExecutionVertex}: the primary datasource that drives query processing. This is everything that isn't
   * broadcast.
   */
  private static IntList baseInputNumbers(final ExecutionContext context)
  {
    final StageDefinition stageDef = context.workOrder().getStageDefinition();
    final List<InputSlice> inputSlices = context.workOrder().getInputs();
    final IntList retVal = new IntArrayList(inputSlices.size());

    for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
      if (!stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
        retVal.add(inputNumber);
      }
    }

    return retVal;
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
