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

package org.apache.druid.msq.exec;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.exec.std.StandardShuffleOperations;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import javax.annotation.Nullable;

/**
 * All the things needed for {@link StageProcessor#execute(ExecutionContext)} to run the work for a stage.
 */
public interface ExecutionContext
{
  /**
   * Work order to execute.
   */
  WorkOrder workOrder();

  /**
   * Processing thread pool to use.
   */
  FrameProcessorExecutor executor();

  /**
   * Reader for {@link WorkOrder#getInputs()}.
   */
  InputSliceReader inputSliceReader();

  /**
   * For {@link ShuffleKind#GLOBAL_SORT}, a future that resolves to the global {@link ClusterByPartitions} (when known).
   * Used by {@link StandardShuffleOperations#globalSort(ListenableFuture, OutputChannelFactory)}.
   */
  ListenableFuture<ClusterByPartitions> globalClusterByPartitions();

  /**
   * Factory for generating stage output channels.
   */
  OutputChannelFactory outputChannelFactory();

  /**
   * Creates a buffered intermediate output channel, such as for spilling temporary streams to disk. For a non-buffered
   * intermediate channel, use {@link BlockingQueueFrameChannel#minimal()} instead.
   */
  OutputChannelFactory makeIntermediateOutputChannelFactory(String name);

  /**
   * Services and objects for the functioning of various processors.
   */
  FrameContext frameContext();

  /**
   * Facility for tracking query counters and metrics.
   */
  CounterTracker counters();

  /**
   * Number of threads available in {@link #executor()}.
   */
  int threadCount();

  /**
   * Cancellation ID that must be provided to {@link FrameProcessorExecutor} when running work.
   */
  String cancellationId();

  /**
   * Bouncer that must be used when submitting work to the {@link FrameProcessorExecutor} that uses
   * {@link ProcessingBuffers}.
   */
  Bouncer processingBouncer();

  /**
   * Callback that must be called when input is done being read. This is essential for two reasons:
   * (1) If the prior stage ran with {@link OutputChannelMode#MEMORY}, this informs the controller that it can shut
   * down the prior stage.
   * (2) With {@link ShuffleKind#GLOBAL_SORT}, this provides statistics that are used to determine global boundaries.
   *
   * Typically called by {@link StandardShuffleOperations#gatherResultKeyStatisticsIfNeeded(ListenableFuture)}.
   */
  void onDoneReadingInput(@Nullable ClusterByStatisticsSnapshot snapshot);

  /**
   * Callback to report a nonfatal warning.
   */
  void onWarning(Throwable t);
}
