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
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.processor.FrameChannelMerger;
import org.apache.druid.frame.processor.FrameChannelMixer;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.counters.CpuCounters;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.FrameWriterSpec;
import org.apache.druid.msq.exec.InputChannelFactory;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper class for reading {@link ReadablePartition} as a single {@link ReadableFrameChannel}, in the
 * standard manner. Partitions located on a single worker are read as-is. Partitions located on multiple
 * workers are either mixed or stream-merged depending on the {@link ShuffleSpec}.
 */
public class StandardPartitionReader
{
  private final QueryDefinition queryDef;
  private final InputChannelFactory inputChannelFactory;
  private final FrameWriterSpec frameWriterSpec;
  private final FrameProcessorExecutor exec;
  private final String cancellationId;
  @Nullable
  private final CounterTracker counters;
  private final MemoryAllocatorFactory allocatorFactory;

  public StandardPartitionReader(ExecutionContext executionContext)
  {
    this.queryDef = executionContext.workOrder().getQueryDefinition();
    this.inputChannelFactory = executionContext.inputChannelFactory();
    this.frameWriterSpec = executionContext.frameContext().frameWriterSpec();
    this.exec = executionContext.executor();
    this.cancellationId = executionContext.cancellationId();
    this.counters = executionContext.counters();
    this.allocatorFactory =
        new ArenaMemoryAllocatorFactory(executionContext.frameContext().memoryParameters().getFrameSize());
  }

  public StandardPartitionReader(
      final QueryDefinition queryDef,
      final InputChannelFactory inputChannelFactory,
      final FrameWriterSpec frameWriterSpec,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      @Nullable final CounterTracker counters,
      final MemoryAllocatorFactory allocatorFactory
  )
  {
    this.queryDef = queryDef;
    this.inputChannelFactory = inputChannelFactory;
    this.frameWriterSpec = frameWriterSpec;
    this.exec = exec;
    this.cancellationId = cancellationId;
    this.counters = counters;
    this.allocatorFactory = allocatorFactory;
  }

  public ReadableFrameChannel openChannel(final ReadablePartition readablePartition) throws IOException
  {
    final StageDefinition stageDef = queryDef.getStageDefinition(readablePartition.getStageNumber());
    final ClusterBy clusterBy = stageDef.getClusterBy();
    final boolean isSorted = clusterBy.sortable() && (clusterBy.getColumns().size() - clusterBy.getBucketByCount() > 0);

    if (isSorted) {
      return openSorted(stageDef, readablePartition);
    } else {
      return openUnsorted(stageDef, readablePartition);
    }
  }

  public FrameReader frameReader(final int stageNumber)
  {
    return queryDef.getStageDefinition(stageNumber).getFrameReader();
  }

  private ReadableFrameChannel openSorted(
      final StageDefinition stageDefinition,
      final ReadablePartition readablePartition
  ) throws IOException
  {
    // Note: this method uses a single FrameChannelMerger, not a SuperSorter, for efficiency. (Currently, SuperSorter
    // is always multi-level and always uses disk.)
    final BlockingQueueFrameChannel queueChannel = BlockingQueueFrameChannel.minimal();

    final List<ReadableFrameChannel> channels = openChannels(
        stageDefinition.getId(),
        readablePartition
    );

    if (channels.size() == 1) {
      return Iterables.getOnlyElement(channels);
    } else {
      final FrameChannelMerger merger = new FrameChannelMerger(
          channels,
          stageDefinition.getFrameReader(),
          queueChannel.writable(),
          FrameWriters.makeFrameWriterFactory(
              frameWriterSpec.getRowBasedFrameType(),
              new SingleMemoryAllocatorFactory(allocatorFactory.newAllocator()),
              stageDefinition.getFrameReader().signature(),
              Collections.emptyList(),
              frameWriterSpec.getRemoveNullBytes()
          ),
          stageDefinition.getSortKey(),
          null,
          -1
      );

      // Discard future, since there is no need to keep it. We aren't interested in its return value. If it fails,
      // downstream processors are notified through fail(e) on in-memory channels. If we need to cancel it, we use
      // the cancellationId.
      exec.runFully(
          counters == null ? merger : counters.trackCpu(merger, CpuCounters.LABEL_MERGE_INPUT),
          cancellationId
      );

      return queueChannel.readable();
    }
  }

  private ReadableFrameChannel openUnsorted(
      final StageDefinition stageDefinition,
      final ReadablePartition readablePartition
  ) throws IOException
  {
    final List<ReadableFrameChannel> channels = openChannels(
        stageDefinition.getId(),
        readablePartition
    );

    if (channels.size() == 1) {
      return Iterables.getOnlyElement(channels);
    } else {
      final BlockingQueueFrameChannel queueChannel = BlockingQueueFrameChannel.minimal();
      final FrameChannelMixer muxer = new FrameChannelMixer(channels, queueChannel.writable());

      // Discard future, since there is no need to keep it. We aren't interested in its return value. If it fails,
      // downstream processors are notified through fail(e) on in-memory channels. If we need to cancel it, we use
      // the cancellationId.
      exec.runFully(
          counters == null ? muxer : counters.trackCpu(muxer, CpuCounters.LABEL_MERGE_INPUT),
          cancellationId
      );

      return queueChannel.readable();
    }
  }

  private List<ReadableFrameChannel> openChannels(
      final StageId stageId,
      final ReadablePartition readablePartition
  ) throws IOException
  {
    final List<ReadableFrameChannel> channels = new ArrayList<>();

    try {
      for (final int workerNumber : readablePartition.getWorkerNumbers()) {
        channels.add(
            inputChannelFactory.openChannel(
                stageId,
                workerNumber,
                readablePartition.getPartitionNumber()
            )
        );
      }

      return channels;
    }
    catch (Exception e) {
      // Close all channels opened so far before throwing the exception.
      for (final ReadableFrameChannel channel : channels) {
        try {
          channel.close();
        }
        catch (Exception e2) {
          e.addSuppressed(e2);
        }
      }

      throw e;
    }
  }
}
