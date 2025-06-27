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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.exec.DataServerQueryHandlerFactory;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.msq.exec.ProcessingBuffersSet;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.exec.WorkerStorageParameters;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.server.DruidNode;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.io.File;

/**
 * Dart implementation of {@link WorkerContext}.
 * Each instance is scoped to a query.
 */
public class DartWorkerContext implements WorkerContext
{
  private final String queryId;
  private final String controllerHost;
  private final WorkerId workerId;
  private final DruidNode selfNode;
  private final ObjectMapper jsonMapper;
  private final PolicyEnforcer policyEnforcer;
  private final Injector injector;
  private final DartWorkerClient workerClient;
  private final DruidProcessingConfig processingConfig;
  private final SegmentWrangler segmentWrangler;
  private final GroupingEngine groupingEngine;
  private final DataSegmentProvider dataSegmentProvider;
  private final MemoryIntrospector memoryIntrospector;
  private final ProcessingBuffersProvider processingBuffersProvider;
  private final Outbox<ControllerMessage> outbox;
  private final File tempDir;
  private final QueryContext queryContext;

  /**
   * Lazy initialized upon call to {@link #frameContext(WorkOrder)}.
   */
  @MonotonicNonNull
  private volatile ResourceHolder<ProcessingBuffersSet> processingBuffersSet;
  private final DataServerQueryHandlerFactory dataServerQueryHandlerFactory;

  DartWorkerContext(
      final String queryId,
      final String controllerHost,
      final DruidNode selfNode,
      final ObjectMapper jsonMapper,
      final PolicyEnforcer policyEnforcer,
      final Injector injector,
      final DartWorkerClient workerClient,
      final DruidProcessingConfig processingConfig,
      final SegmentWrangler segmentWrangler,
      final GroupingEngine groupingEngine,
      final DataSegmentProvider dataSegmentProvider,
      final MemoryIntrospector memoryIntrospector,
      final ProcessingBuffersProvider processingBuffersProvider,
      final Outbox<ControllerMessage> outbox,
      final File tempDir,
      final QueryContext queryContext,
      final DataServerQueryHandlerFactory dataServerQueryHandlerFactory
  )
  {
    this.queryId = queryId;
    this.controllerHost = controllerHost;
    this.dataServerQueryHandlerFactory = dataServerQueryHandlerFactory;
    this.workerId = WorkerId.fromDruidNode(selfNode, queryId);
    this.selfNode = selfNode;
    this.jsonMapper = jsonMapper;
    this.policyEnforcer = policyEnforcer;
    this.injector = injector;
    this.workerClient = workerClient;
    this.processingConfig = processingConfig;
    this.segmentWrangler = segmentWrangler;
    this.groupingEngine = groupingEngine;
    this.dataSegmentProvider = dataSegmentProvider;
    this.memoryIntrospector = memoryIntrospector;
    this.processingBuffersProvider = processingBuffersProvider;
    this.outbox = outbox;
    this.tempDir = tempDir;
    this.queryContext = Preconditions.checkNotNull(queryContext, "queryContext");
  }

  @Override
  public String queryId()
  {
    return queryId;
  }

  @Override
  public String workerId()
  {
    return workerId.toString();
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  @Override
  public PolicyEnforcer policyEnforcer()
  {
    return policyEnforcer;
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void registerWorker(Worker worker, Closer closer)
  {
    closer.register(() -> {
      synchronized (this) {
        if (processingBuffersSet != null) {
          processingBuffersSet.close();
          processingBuffersSet = null;
        }
      }

      workerClient.close();
    });
  }

  @Override
  public int maxConcurrentStages()
  {
    final int retVal = MultiStageQueryContext.getMaxConcurrentStagesWithDefault(queryContext, -1);
    if (retVal <= 0) {
      throw new IAE("Illegal maxConcurrentStages[%s]", retVal);
    }
    return retVal;
  }

  @Override
  public ControllerClient makeControllerClient()
  {
    return new DartControllerClient(outbox, queryId, controllerHost);
  }

  @Override
  public WorkerClient makeWorkerClient()
  {
    return workerClient;
  }

  @Override
  public File tempDir()
  {
    return tempDir;
  }

  @Override
  public FrameContext frameContext(WorkOrder workOrder)
  {
    if (processingBuffersSet == null) {
      synchronized (this) {
        if (processingBuffersSet == null) {
          processingBuffersSet = processingBuffersProvider.acquire(
              workOrder.getQueryDefinition(),
              maxConcurrentStages()
          );
        }
      }
    }

    final WorkerMemoryParameters memoryParameters =
        WorkerMemoryParameters.createProductionInstance(
            workOrder,
            memoryIntrospector,
            maxConcurrentStages()
        );

    final WorkerStorageParameters storageParameters = WorkerStorageParameters.createInstance(-1, false);

    return new DartFrameContext(
        workOrder.getStageDefinition().getId(),
        this,
        segmentWrangler,
        groupingEngine,
        dataSegmentProvider,
        processingBuffersSet.get().acquireForStage(workOrder.getStageDefinition()),
        memoryParameters,
        storageParameters,
        dataServerQueryHandlerFactory
    );
  }

  @Override
  public int threadCount()
  {
    return processingConfig.getNumThreads();
  }

  @Override
  public DataServerQueryHandlerFactory dataServerQueryHandlerFactory()
  {
    return dataServerQueryHandlerFactory;
  }

  @Override
  public boolean includeAllCounters()
  {
    // The context parameter "includeAllCounters" is meant to assist with backwards compatibility for versions prior
    // to Druid 31. Dart didn't exist prior to Druid 31, so there is no need for it here. Always emit all counters.
    return true;
  }

  @Override
  public DruidNode selfNode()
  {
    return selfNode;
  }
}
