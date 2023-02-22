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

import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.channel.ReadableConcatFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.InputSlices;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.input.ReadableInputs;
import org.apache.druid.msq.input.external.ExternalInputSlice;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class BaseLeafFrameProcessorFactory extends BaseFrameProcessorFactory
{
  private static final Logger log = new Logger(BaseLeafFrameProcessorFactory.class);

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
    // BaseLeafFrameProcessorFactory is used for native Druid queries, where the following input cases can happen:
    //   1) Union datasources: N nonbroadcast inputs, which are are treated as one big input
    //   2) Join datasources: one nonbroadcast input, N broadcast inputs
    //   3) All other datasources: single input

    final int totalProcessors = InputSlices.getNumNonBroadcastReadableInputs(
        inputSlices,
        inputSliceReader,
        stageDefinition.getBroadcastInputNumbers()
    );

    if (totalProcessors == 0) {
      return new ProcessorsAndChannels<>(Sequences.empty(), OutputChannels.none());
    }

    final int outstandingProcessors;

    if (hasParquet(inputSlices)) {
      // This is a workaround for memory use in ParquetFileReader, which loads up an entire row group into memory as
      // part of its normal operation. Row groups can be quite large (like, 1GB large) so this is a major source of
      // unaccounted-for memory use during ingestion and query of external data. We are trying to prevent memory
      // overload by running only a single processor at once.
      outstandingProcessors = 1;
    } else {
      outstandingProcessors = Math.min(totalProcessors, maxOutstandingProcessors);
    }

    final AtomicReference<Queue<MemoryAllocator>> allocatorQueueRef =
        new AtomicReference<>(new ArrayDeque<>(outstandingProcessors));
    final AtomicReference<Queue<WritableFrameChannel>> channelQueueRef =
        new AtomicReference<>(new ArrayDeque<>(outstandingProcessors));
    final List<OutputChannel> outputChannels = new ArrayList<>(outstandingProcessors);

    for (int i = 0; i < outstandingProcessors; i++) {
      final OutputChannel outputChannel = outputChannelFactory.openChannel(0 /* Partition number doesn't matter */);
      outputChannels.add(outputChannel);
      channelQueueRef.get().add(outputChannel.getWritableChannel());
      allocatorQueueRef.get().add(outputChannel.getFrameMemoryAllocator());
    }

    // Read all base inputs in separate processors, one per processor.
    final Sequence<ReadableInput> processorBaseInputs = readBaseInputs(
        stageDefinition,
        inputSlices,
        inputSliceReader,
        counters,
        warningPublisher
    );

    final Sequence<FrameProcessor<Long>> processors = processorBaseInputs.map(
        processorBaseInput -> {
          // For each processor, we are rebuilding the broadcast table again which is wasteful. This can be pushed
          // up to the factory level
          final Int2ObjectMap<ReadableInput> sideChannels =
              readBroadcastInputs(stageDefinition, inputSlices, inputSliceReader, counters, warningPublisher);

          return makeProcessor(
              processorBaseInput,
              sideChannels,
              makeLazyResourceHolder(
                  channelQueueRef,
                  channel -> {
                    try {
                      channel.close();
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
              ),
              makeLazyResourceHolder(allocatorQueueRef, ignored -> {}),
              stageDefinition.getSignature(),
              stageDefinition.getClusterBy(),
              frameContext
          );
        }
    ).withBaggage(
        () -> {
          final Queue<WritableFrameChannel> channelQueue;
          synchronized (channelQueueRef) {
            // Set to null so any channels returned by outstanding workers are immediately closed.
            channelQueue = channelQueueRef.getAndSet(null);
          }

          WritableFrameChannel c;
          while ((c = channelQueue.poll()) != null) {
            try {
              c.close();
            }
            catch (Throwable e) {
              log.warn(e, "Error encountered while closing channel for [%s]", this);
            }
          }
        }
    );

    return new ProcessorsAndChannels<>(processors, OutputChannels.wrapReadOnly(outputChannels));
  }

  /**
   * Read base inputs, where "base" is meant in the same sense as in
   * {@link org.apache.druid.query.planning.DataSourceAnalysis}: the primary datasource that drives query processing.
   */
  private static Sequence<ReadableInput> readBaseInputs(
      final StageDefinition stageDef,
      final List<InputSlice> inputSlices,
      final InputSliceReader inputSliceReader,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final List<Sequence<ReadableInput>> sequences = new ArrayList<>();

    for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
      if (!stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
        final int i = inputNumber;
        final Sequence<ReadableInput> sequence =
            Sequences.simple(inputSliceReader.attach(i, inputSlices.get(i), counters, warningPublisher));
        sequences.add(sequence);
      }
    }

    return Sequences.concat(sequences);
  }

  /**
   * Reads all broadcast inputs, which must be {@link StageInputSlice}. The execution framework supports broadcasting
   * other types of inputs, but QueryKit does not use them at this time.
   *
   * Returns a map of input number -> channel containing all data for that input number.
   */
  private static Int2ObjectMap<ReadableInput> readBroadcastInputs(
      final StageDefinition stageDef,
      final List<InputSlice> inputSlices,
      final InputSliceReader inputSliceReader,
      final CounterTracker counterTracker,
      final Consumer<Throwable> warningPublisher
  )
  {
    final Int2ObjectMap<ReadableInput> broadcastInputs = new Int2ObjectAVLTreeMap<>();

    try {
      for (int inputNumber = 0; inputNumber < inputSlices.size(); inputNumber++) {
        if (stageDef.getBroadcastInputNumbers().contains(inputNumber)) {
          // QueryKit only uses StageInputSlice at this time.
          final StageInputSlice slice = (StageInputSlice) inputSlices.get(inputNumber);
          final ReadableInputs readableInputs =
              inputSliceReader.attach(inputNumber, slice, counterTracker, warningPublisher);

          if (!readableInputs.isChannelBased()) {
            // QueryKit limitation: broadcast inputs must be channels.
            throw new ISE("Broadcast inputs must be channels");
          }

          final ReadableFrameChannel channel = ReadableConcatFrameChannel.open(
              Iterators.transform(readableInputs.iterator(), ReadableInput::getChannel)
          );
          broadcastInputs.put(inputNumber, ReadableInput.channel(channel, readableInputs.frameReader(), null));
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

  protected abstract FrameProcessor<Long> makeProcessor(
      ReadableInput baseInput,
      Int2ObjectMap<ReadableInput> sideChannels,
      ResourceHolder<WritableFrameChannel> outputChannelSupplier,
      ResourceHolder<MemoryAllocator> allocatorSupplier,
      RowSignature signature,
      ClusterBy clusterBy,
      FrameContext providerThingy
  );

  private static <T> ResourceHolder<T> makeLazyResourceHolder(
      final AtomicReference<Queue<T>> queueRef,
      final Consumer<T> backupCloser
  )
  {
    return new LazyResourceHolder<>(
        () -> {
          final T resource;

          synchronized (queueRef) {
            resource = queueRef.get().poll();
          }

          return Pair.of(
              resource,
              () -> {
                synchronized (queueRef) {
                  final Queue<T> queue = queueRef.get();
                  if (queue != null) {
                    queue.add(resource);
                    return;
                  }
                }

                // Queue was null
                backupCloser.accept(resource);
              }
          );
        }
    );
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
