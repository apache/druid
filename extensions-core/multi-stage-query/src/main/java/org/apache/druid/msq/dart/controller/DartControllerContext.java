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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.msq.dart.worker.DartWorkerClient;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerMemoryParameters;
import org.apache.druid.msq.exec.MSQMetricUtils;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.exec.WorkerFailureListener;
import org.apache.druid.msq.exec.WorkerManager;
import org.apache.druid.msq.indexing.IndexerControllerContext;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Dart implementation of {@link ControllerContext}.
 * Each instance is scoped to a query.
 */
public class DartControllerContext implements ControllerContext
{
  /**
   * Default for {@link ControllerQueryKernelConfig#getMaxConcurrentStages()}.
   */
  public static final int DEFAULT_MAX_CONCURRENT_STAGES = 2;

  /**
   * Default for {@link MultiStageQueryContext#getTargetPartitionsPerWorkerWithDefault(QueryContext, int)}.
   */
  public static final int DEFAULT_TARGET_PARTITIONS_PER_WORKER = 1;

  /**
   * Context parameter for maximum number of nonleaf workers.
   */
  public static final String CTX_MAX_NON_LEAF_WORKER_COUNT = "maxNonLeafWorkers";

  /**
   * Default to scatter/gather style: fan in to a single worker after the leaf stage(s).
   */
  public static final int DEFAULT_MAX_NON_LEAF_WORKER_COUNT = 1;

  public static final SegmentSource DEFAULT_SEGMENT_SOURCE = SegmentSource.REALTIME;

  private final Injector injector;
  private final ObjectMapper jsonMapper;
  private final DruidNode selfNode;
  private final DartWorkerClient workerClient;
  private final TimelineServerView serverView;
  private final MemoryIntrospector memoryIntrospector;
  private final QueryContext context;
  private final ServiceEmitter emitter;

  public DartControllerContext(
      final Injector injector,
      final ObjectMapper jsonMapper,
      final DruidNode selfNode,
      final DartWorkerClient workerClient,
      final MemoryIntrospector memoryIntrospector,
      final TimelineServerView serverView,
      final ServiceEmitter emitter,
      final QueryContext context
  )
  {
    this.injector = injector;
    this.jsonMapper = jsonMapper;
    this.selfNode = selfNode;
    this.workerClient = workerClient;
    this.serverView = serverView;
    this.memoryIntrospector = memoryIntrospector;
    this.context = context;
    this.emitter = emitter;
  }

  @Override
  public ControllerQueryKernelConfig queryKernelConfig(
      final String queryId,
      final MSQSpec querySpec
  )
  {
    final List<DruidServerMetadata> servers = serverView.getDruidServerMetadatas();

    // Lock in the list of workers when creating the kernel config. There is a race here: the serverView itself is
    // allowed to float. If a segment moves to a new server that isn't part of our list after the WorkerManager is
    // created, we won't be able to find a valid server for certain segments. This isn't expected to be a problem,
    // since the serverView is referenced shortly after the worker list is created.
    final List<String> workerIds = new ArrayList<>(servers.size());
    for (final DruidServerMetadata server : servers) {
      if (server.getType() == ServerType.HISTORICAL) {
        workerIds.add(WorkerId.fromDruidServerMetadata(server, queryId).toString());
      }
    }

    // Shuffle workerIds, so we don't bias towards specific servers when running multiple queries concurrently. For any
    // given query, lower-numbered workers tend to do more work, because the controller prefers using lower-numbered
    // workers when maxWorkerCount for a stage is less than the total number of workers.
    Collections.shuffle(workerIds);

    final ControllerMemoryParameters memoryParameters =
        ControllerMemoryParameters.createProductionInstance(
            memoryIntrospector,
            workerIds.size()
        );

    final int maxConcurrentStages = MultiStageQueryContext.getMaxConcurrentStagesWithDefault(
        querySpec.getContext(),
        DEFAULT_MAX_CONCURRENT_STAGES
    );

    Map<String, Object> indexerContext = IndexerControllerContext.makeWorkerContextMap(
        querySpec,
        false,
        maxConcurrentStages
    );
    final ImmutableMap.Builder<String, Object> dartContextBuilder = ImmutableMap.builder();
    dartContextBuilder.putAll(indexerContext);
    dartContextBuilder.put(QueryContexts.CTX_DART_QUERY_ID, querySpec.getContext().get(QueryContexts.CTX_DART_QUERY_ID));

    return ControllerQueryKernelConfig
        .builder()
        .controllerHost(selfNode.getHostAndPortToUse())
        .workerIds(workerIds)
        .pipeline(maxConcurrentStages > 1)
        .destination(TaskReportMSQDestination.instance())
        .maxConcurrentStages(maxConcurrentStages)
        .maxRetainedPartitionSketchBytes(memoryParameters.getPartitionStatisticsMaxRetainedBytes())
        .workerContextMap(dartContextBuilder.build())
        .build();
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void emitMetric(final String metric, Map<String, Object> dimensions, final Number value)
  {
    ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    MSQMetricUtils.setDartQueryIdDimensions(metricBuilder, context);
    dimensions.forEach(metricBuilder::setDimension);
    emitter.emit(metricBuilder.setMetric(metric, value));
  }

  @Override
  public DruidNode selfNode()
  {
    return selfNode;
  }

  @Override
  public InputSpecSlicer newTableInputSpecSlicer(WorkerManager workerManager)
  {
    return DartTableInputSpecSlicer.createFromWorkerIds(workerManager.getWorkerIds(), serverView, context);
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public WorkerManager newWorkerManager(
      String queryId,
      MSQSpec querySpec,
      ControllerQueryKernelConfig queryKernelConfig,
      WorkerFailureListener workerFailureListener
  )
  {
    // We're ignoring WorkerFailureListener. Dart worker failures are routed into the controller by
    // ControllerMessageListener, which receives a notification when a worker goes offline.
    return new DartWorkerManager(queryKernelConfig.getWorkerIds(), workerClient);
  }

  @Override
  public DartWorkerClient newWorkerClient()
  {
    return workerClient;
  }

  @Override
  public void registerController(Controller controller, Closer closer)
  {
    closer.register(workerClient);
  }

  @Override
  public TaskLockType taskLockType()
  {
    throw DruidException.defensive("TaskLockType is not used with class[%s]", getClass().getName());
  }
}
