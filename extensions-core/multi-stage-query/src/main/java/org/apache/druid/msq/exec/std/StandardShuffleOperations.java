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

package org.apache.druid.msq.exec.std;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.FrameChannelHashPartitioner;
import org.apache.druid.frame.processor.FrameChannelMixer;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessorDecorator;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.processor.SuperSorter;
import org.apache.druid.frame.processor.SuperSorterProgressTracker;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.counters.CpuCounters;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.RunWorkOrderListener;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.processor.KeyStatisticsCollectionProcessor;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.util.MultiStageQueryContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class for performing common shuffle-related operations. Used by {@link StageProcessor} implementations.
 */
public class StandardShuffleOperations
{
  private final ExecutionContext executionContext;
  private final WorkOrder workOrder;

  public StandardShuffleOperations(final ExecutionContext executionContext)
  {
    this.executionContext = executionContext;
    this.workOrder = executionContext.workOrder();
  }

  /**
   * Mixes all output from "arg" into a single channel from the provided output channel factory.
   */
  public <T> ListenableFuture<ResultAndChannels<Object>> mix(
      final ListenableFuture<ResultAndChannels<T>> arg,
      final OutputChannelFactory outputChannelFactory
  )
  {
    return transform(
        arg,
        resultAndChannels -> {
          final OutputChannel outputChannel = outputChannelFactory.openChannel(0);

          final FrameChannelMixer mixer =
              new FrameChannelMixer(
                  resultAndChannels.outputChannels().getAllReadableChannels(),
                  outputChannel.getWritableChannel()
              );

          return new ResultAndChannels<>(
              executionContext.executor().runFully(
                  executionContext.counters().trackCpu(mixer, CpuCounters.LABEL_MIX),
                  executionContext.cancellationId()
              ),
              OutputChannels.wrap(Collections.singletonList(outputChannel.readOnly()))
          );
        }
    );
  }

  /**
   * If {@link StageDefinition#mustGatherResultKeyStatistics()}, runs {@link KeyStatisticsCollectionProcessor} on "arg",
   * then calls {@link RunWorkOrderListener#onDoneReadingInput(ClusterByStatisticsSnapshot)} when done.
   *
   * Otherwise, calls the listener as soon as the {@link ResultAndChannels#resultFuture()} from "arg" is available.
   */
  public <T> ListenableFuture<ResultAndChannels<Object>> gatherResultKeyStatisticsIfNeeded(
      final ListenableFuture<ResultAndChannels<T>> arg
  )
  {
    //noinspection unchecked
    return transform(
        arg,
        resultAndChannels -> {
          final StageDefinition stageDefinition = workOrder.getStageDefinition();
          final OutputChannels channels = resultAndChannels.outputChannels();

          if (channels.getAllChannels().isEmpty()) {
            // No data coming out of this stage. Report empty statistics, if the kernel is expecting statistics.
            if (stageDefinition.mustGatherResultKeyStatistics()) {
              executionContext.onDoneReadingInput(ClusterByStatisticsSnapshot.empty());
            } else {
              executionContext.onDoneReadingInput(null);
            }

            // Generate one empty channel so the next part of the pipeline has something to do.
            final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
            channel.writable().close();

            final OutputChannel outputChannel = OutputChannel.readOnly(
                channel.readable(),
                FrameWithPartition.NO_PARTITION
            );

            return new ResultAndChannels<>(
                Futures.immediateFuture(null),
                OutputChannels.wrap(Collections.singletonList(outputChannel))
            );
          } else if (stageDefinition.mustGatherResultKeyStatistics()) {
            //noinspection rawtypes, unchecked
            return (ResultAndChannels) gatherResultKeyStatistics(channels);
          } else {
            // Report "done reading input" when the input future resolves.
            // No need to run any more processors.
            resultAndChannels.resultFuture().addListener(
                () -> executionContext.onDoneReadingInput(null),
                Execs.directExecutor()
            );
            //noinspection unchecked
            return (ResultAndChannels<Object>) resultAndChannels;
          }
        }
    );
  }

  /**
   * Runs a {@link SuperSorter} using {@link StageDefinition#getSortKey()}.
   */
  public <T> ListenableFuture<ResultAndChannels<Object>> globalSort(
      final ListenableFuture<ResultAndChannels<T>> arg,
      final OutputChannelFactory outputChannelFactory
  )
  {
    return transformAsync(
        arg,
        resultAndChannels -> {
          final StageDefinition stageDefinition = workOrder.getStageDefinition();
          final WorkerMemoryParameters memoryParameters = executionContext.frameContext().memoryParameters();
          final SuperSorter sorter = new SuperSorter(
              resultAndChannels.outputChannels().getAllReadableChannels(),
              stageDefinition.getFrameReader(),
              stageDefinition.getSortKey(),
              executionContext.globalClusterByPartitions(),
              executionContext.executor(),
              new FrameProcessorDecorator()
              {
                @Override
                public <T2> FrameProcessor<T2> decorate(FrameProcessor<T2> processor)
                {
                  return executionContext.counters().trackCpu(processor, CpuCounters.LABEL_SORT);
                }
              },
              outputChannelFactory,
              executionContext.makeIntermediateOutputChannelFactory("super-sort"),
              memoryParameters.getSuperSorterConcurrentProcessors(),
              memoryParameters.getSuperSorterMaxChannelsPerMerger(),
              stageDefinition.getShuffleSpec().limitHint(),
              executionContext.cancellationId(),
              executionContext.counters().sortProgress(),
              isRemoveNullBytes()
          );

          return FutureUtils.transform(
              sorter.run(),
              sortedChannels -> new ResultAndChannels<>(Futures.immediateFuture(null), sortedChannels)
          );
        }
    );
  }

  /**
   * Runs a {@link FrameChannelHashPartitioner} using {@link StageDefinition#getSortKey()}. The returned future
   * resolves when the output channels are ready for reading.
   */
  public <T> ListenableFuture<ResultAndChannels<Object>> hashPartition(
      final ListenableFuture<ResultAndChannels<T>> arg,
      final OutputChannelFactory outputChannelFactory,
      final boolean isOutputChannelBuffered
  )
  {
    return transformAsync(
        arg,
        resultAndChannels -> {
          final ShuffleSpec shuffleSpec = workOrder.getStageDefinition().getShuffleSpec();
          final int partitions = shuffleSpec.partitionCount();

          final List<OutputChannel> outputChannels = new ArrayList<>();

          for (int i = 0; i < partitions; i++) {
            outputChannels.add(outputChannelFactory.openChannel(i));
          }

          final FrameChannelHashPartitioner partitioner = new FrameChannelHashPartitioner(
              resultAndChannels.outputChannels().getAllReadableChannels(),
              outputChannels.stream().map(OutputChannel::getWritableChannel).collect(Collectors.toList()),
              workOrder.getStageDefinition().getFrameReader(),
              workOrder.getStageDefinition().getClusterBy().getColumns().size(),
              FrameWriters.makeRowBasedFrameWriterFactory(
                  new ArenaMemoryAllocatorFactory(executionContext.frameContext().memoryParameters().getFrameSize()),
                  workOrder.getStageDefinition().getSignature(),
                  workOrder.getStageDefinition().getSortKey(),
                  isRemoveNullBytes()
              )
          );

          final ListenableFuture<Long> partitionerFuture =
              executionContext.executor().runFully(
                  executionContext.counters().trackCpu(partitioner, CpuCounters.LABEL_HASH_PARTITION),
                  executionContext.cancellationId()
              );

          final ResultAndChannels<Long> retVal =
              new ResultAndChannels<>(partitionerFuture, OutputChannels.wrap(outputChannels));

          if (isOutputChannelBuffered) {
            return FutureUtils.transform(partitionerFuture, ignored -> retVal);
          } else {
            return Futures.immediateFuture(retVal);
          }
        }
    );
  }

  /**
   * Runs a sequence of {@link SuperSorter}, operating on each output channel from "arg" in order, one at a time.
   */
  public <T> ListenableFuture<ResultAndChannels<Object>> localSort(
      final ListenableFuture<ResultAndChannels<T>> arg,
      final OutputChannelFactory outputChannelFactory
  )
  {
    return transformAsync(
        arg,
        resultAndChannels -> {
          final StageDefinition stageDefinition = workOrder.getStageDefinition();
          final OutputChannels channels = resultAndChannels.outputChannels();
          final List<ListenableFuture<OutputChannel>> sortedChannelFutures = new ArrayList<>();

          ListenableFuture<OutputChannel> nextFuture = Futures.immediateFuture(null);

          for (final OutputChannel channel : channels.getAllChannels()) {
            final File sorterTmpDir = executionContext.frameContext().tempDir(
                StringUtils.format("hash-parts-super-sort-%06d", channel.getPartitionNumber())
            );

            FileUtils.mkdirp(sorterTmpDir);

            // SuperSorter will try to write to output partition zero; we remap it to the correct partition number.
            final OutputChannelFactory partitionOverrideOutputChannelFactory = new OutputChannelFactory()
            {
              @Override
              public OutputChannel openChannel(int expectedZero) throws IOException
              {
                if (expectedZero != 0) {
                  throw new ISE("Unexpected part [%s]", expectedZero);
                }

                return outputChannelFactory.openChannel(channel.getPartitionNumber());
              }

              @Override
              public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead)
              {
                throw new UnsupportedOperationException();
              }

              @Override
              public OutputChannel openNilChannel(int expectedZero)
              {
                if (expectedZero != 0) {
                  throw new ISE("Unexpected part [%s]", expectedZero);
                }

                return outputChannelFactory.openNilChannel(channel.getPartitionNumber());
              }
            };

            // Chain futures so we only sort one partition at a time.
            nextFuture = Futures.transformAsync(
                nextFuture,
                ignored -> {
                  final SuperSorter sorter = new SuperSorter(
                      Collections.singletonList(channel.getReadableChannel()),
                      stageDefinition.getFrameReader(),
                      stageDefinition.getSortKey(),
                      Futures.immediateFuture(ClusterByPartitions.oneUniversalPartition()),
                      executionContext.executor(),
                      new FrameProcessorDecorator()
                      {
                        @Override
                        public <T2> FrameProcessor<T2> decorate(FrameProcessor<T2> processor)
                        {
                          return executionContext.counters().trackCpu(processor, CpuCounters.LABEL_SORT);
                        }
                      },
                      partitionOverrideOutputChannelFactory,
                      executionContext.makeIntermediateOutputChannelFactory(
                          StringUtils.format("hash-parts-super-sort-%06d", channel.getPartitionNumber())),
                      1,
                      2,
                      ShuffleSpec.UNLIMITED,
                      executionContext.cancellationId(),

                      // Tracker is not actually tracked, since it doesn't quite fit into the way we report counters.
                      // There's a single SuperSorterProgressTrackerCounter per worker, but workers that do local
                      // sorting have a SuperSorter per partition.
                      new SuperSorterProgressTracker(),
                      isRemoveNullBytes()
                  );

                  return FutureUtils.transform(sorter.run(), r -> Iterables.getOnlyElement(r.getAllChannels()));
                },
                MoreExecutors.directExecutor()
            );

            sortedChannelFutures.add(nextFuture);
          }

          return FutureUtils.transform(
              Futures.allAsList(sortedChannelFutures),
              sortedChannels -> new ResultAndChannels<>(
                  Futures.immediateFuture(null),
                  OutputChannels.wrap(sortedChannels)
              )
          );
        }
    );
  }

  /**
   * Runs {@link KeyStatisticsCollectionProcessor} on the provided channels.
   */
  private ResultAndChannels<ClusterByStatisticsCollector> gatherResultKeyStatistics(final OutputChannels channels)
  {
    final StageDefinition stageDefinition = workOrder.getStageDefinition();
    final List<OutputChannel> retVal = new ArrayList<>();
    final int numOutputChannels = channels.getAllChannels().size();
    final List<KeyStatisticsCollectionProcessor> processors = new ArrayList<>(numOutputChannels);

    // Max retained bytes total.
    final int maxRetainedBytes =
        executionContext.frameContext().memoryParameters().getPartitionStatisticsMaxRetainedBytes();

    // Divide the total by two: half for the per-processor collectors together, half for the combined collector.
    // Then divide by numOutputChannels: one portion per processor.
    final int maxRetainedBytesPerChannel = maxRetainedBytes / 2 / numOutputChannels;

    for (final OutputChannel outputChannel : channels.getAllChannels()) {
      final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
      retVal.add(OutputChannel.readOnly(channel.readable(), outputChannel.getPartitionNumber()));

      processors.add(
          new KeyStatisticsCollectionProcessor(
              outputChannel.getReadableChannel(),
              channel.writable(),
              stageDefinition.getFrameReader(),
              stageDefinition.getClusterBy(),
              stageDefinition.createResultKeyStatisticsCollector(maxRetainedBytesPerChannel)
          )
      );
    }

    final ListenableFuture<ClusterByStatisticsCollector> clusterByStatisticsCollectorFuture =
        executionContext.executor().runAllFully(
            executionContext.counters().trackCpu(
                ProcessorManagers.of(processors)
                                 .withAccumulation(
                                     stageDefinition.createResultKeyStatisticsCollector(
                                         // Divide by two: half for the per-processor collectors, half for the
                                         // combined collector.
                                         maxRetainedBytes / 2
                                     ),
                                     ClusterByStatisticsCollector::addAll
                                 ),
                CpuCounters.LABEL_KEY_STATISTICS
            ),
            // Run all processors simultaneously. They are lightweight and this keeps things moving.
            processors.size(),
            Bouncer.unlimited(),
            executionContext.cancellationId()
        );

    Futures.addCallback(
        clusterByStatisticsCollectorFuture,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(final ClusterByStatisticsCollector result)
          {
            executionContext.onDoneReadingInput(result.snapshot());
          }

          @Override
          public void onFailure(Throwable t)
          {
            // Nothing special.
          }
        },
        Execs.directExecutor()
    );

    return new ResultAndChannels<>(
        clusterByStatisticsCollectorFuture,
        OutputChannels.wrap(retVal)
    );
  }

  private boolean isRemoveNullBytes()
  {
    return MultiStageQueryContext.removeNullBytes(workOrder.getWorkerContext());
  }

  private static <T, R> ListenableFuture<ResultAndChannels<Object>> transform(
      final ListenableFuture<ResultAndChannels<T>> resultAndChannels,
      final ExceptionalFunction<ResultAndChannels<T>, ResultAndChannels<R>> fn
  )
  {
    return transformAsync(
        resultAndChannels,
        channels -> Futures.immediateFuture(fn.apply(channels))
    );
  }

  private static <T, R> ListenableFuture<ResultAndChannels<Object>> transformAsync(
      final ListenableFuture<ResultAndChannels<T>> resultAndChannels,
      final ExceptionalFunction<ResultAndChannels<T>, ListenableFuture<ResultAndChannels<R>>> fn
  )
  {
    final ListenableFuture<ResultAndChannels<R>> retVal = FutureUtils.transform(
        FutureUtils.transformAsync(
            resultAndChannels,
            fn::apply
        ),
        newResultAndChannels -> new ResultAndChannels<>(
            newResultAndChannels.resultFuture(),
            newResultAndChannels.outputChannels().readOnly()
        )
    );

    //noinspection unchecked, rawtypes
    return (ListenableFuture) retVal;
  }

  private interface ExceptionalFunction<T, R>
  {
    R apply(T t) throws Exception;
  }
}
