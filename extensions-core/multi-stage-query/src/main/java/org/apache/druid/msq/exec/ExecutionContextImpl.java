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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Standard implementation of {@link ExecutionContext} built by {@link RunWorkOrder}.
 */
public class ExecutionContextImpl implements ExecutionContext
{
  private final WorkOrder workOrder;
  private final FrameProcessorExecutor executor;
  private final InputSliceReader inputSliceReader;
  private final IntermediateOutputChannelFactoryMaker intermediateOutputChannelFactoryMaker;
  private final OutputChannelFactory outputChannelFactory;
  private final SettableFuture<ClusterByPartitions> globalClusterByPartitionsFuture;
  private final FrameContext frameContext;
  private final CounterTracker counters;
  private final int maxOutstandingProcessors;
  private final String cancellationId;
  private final RunWorkOrderListener listener;
  private final Set<String> intermediateOutputChannelFactoryNames = Sets.newConcurrentHashSet();

  ExecutionContextImpl(
      final WorkOrder workOrder,
      final FrameProcessorExecutor executor,
      final InputSliceReader inputSliceReader,
      final IntermediateOutputChannelFactoryMaker intermediateOutputChannelFactoryMaker,
      final OutputChannelFactory outputChannelFactory,
      final SettableFuture<ClusterByPartitions> globalClusterByPartitionsFuture,
      final FrameContext frameContext,
      final CounterTracker counters,
      final int maxOutstandingProcessors,
      final String cancellationId,
      final RunWorkOrderListener listener
  )
  {
    this.workOrder = workOrder;
    this.executor = executor;
    this.inputSliceReader = inputSliceReader;
    this.intermediateOutputChannelFactoryMaker = intermediateOutputChannelFactoryMaker;
    this.outputChannelFactory = outputChannelFactory;
    this.globalClusterByPartitionsFuture = globalClusterByPartitionsFuture;
    this.frameContext = frameContext;
    this.counters = counters;
    this.maxOutstandingProcessors = maxOutstandingProcessors;
    this.cancellationId = cancellationId;
    this.listener = listener;
  }

  @Override
  public WorkOrder workOrder()
  {
    return workOrder;
  }

  @Override
  public FrameProcessorExecutor executor()
  {
    return executor;
  }

  @Override
  public InputSliceReader inputSliceReader()
  {
    return inputSliceReader;
  }

  @Override
  public ListenableFuture<ClusterByPartitions> globalClusterByPartitions()
  {
    return globalClusterByPartitionsFuture;
  }

  @Override
  public OutputChannelFactory outputChannelFactory()
  {
    return outputChannelFactory;
  }

  @Override
  public OutputChannelFactory makeIntermediateOutputChannelFactory(String name)
  {
    if (intermediateOutputChannelFactoryNames.add(name)) {
      return intermediateOutputChannelFactoryMaker.makeFactory(name);
    } else {
      throw DruidException.defensive("Cannot use intermediate output channel factory name[%s] multiple times", name);
    }
  }

  @Override
  public FrameContext frameContext()
  {
    return frameContext;
  }

  @Override
  public CounterTracker counters()
  {
    return counters;
  }

  @Override
  public int threadCount()
  {
    return maxOutstandingProcessors;
  }

  @Override
  public String cancellationId()
  {
    return cancellationId;
  }

  @Override
  public Bouncer processingBouncer()
  {
    if (workOrder.getStageDefinition().getProcessor().usesProcessingBuffers()) {
      return frameContext.processingBuffers().getBouncer();
    } else {
      return Bouncer.unlimited();
    }
  }

  @Override
  public void onDoneReadingInput(@Nullable ClusterByStatisticsSnapshot snapshot)
  {
    listener.onDoneReadingInput(snapshot);
  }

  @Override
  public void onWarning(Throwable t)
  {
    listener.onWarning(t);
  }

  interface IntermediateOutputChannelFactoryMaker
  {
    OutputChannelFactory makeFactory(String name);
  }
}
