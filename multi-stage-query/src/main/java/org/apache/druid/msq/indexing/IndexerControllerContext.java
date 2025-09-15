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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.frame.FrameType;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerMemoryParameters;
import org.apache.druid.msq.exec.MSQMetriceEventBuilder;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerFailureListener;
import org.apache.druid.msq.exec.WorkerManager;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig;
import org.apache.druid.msq.indexing.client.ControllerChatHandler;
import org.apache.druid.msq.indexing.client.IndexerWorkerClient;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.realtime.ChatHandler;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.StorageConnectorProvider;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Implementation for {@link ControllerContext} required to run multi-stage queries as indexing tasks.
 */
public class IndexerControllerContext implements ControllerContext
{
  public static final int DEFAULT_MAX_CONCURRENT_STAGES = 1;
  public static final SegmentSource DEFAULT_SEGMENT_SOURCE = SegmentSource.NONE;

  private static final Logger log = new Logger(IndexerControllerContext.class);

  private final MSQControllerTask task;
  private final TaskLockType taskLockType;
  private final String taskDataSource;
  private final QueryContext taskQuerySpecContext;
  private final Map<String, Object> taskContext;
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final ServiceClientFactory clientFactory;
  private final OverlordClient overlordClient;
  private final MemoryIntrospector memoryIntrospector;

  public IndexerControllerContext(
      final MSQControllerTask task,
      final TaskToolbox toolbox,
      final Injector injector,
      final ServiceClientFactory clientFactory,
      final OverlordClient overlordClient
  )
  {
    this.task = task;
    this.taskLockType = task.getTaskLockType();
    this.taskDataSource = task.getDataSource();
    this.taskQuerySpecContext = task.getQuerySpec().getContext();
    this.taskContext = task.getContext();
    this.toolbox = toolbox;
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
    this.memoryIntrospector = injector.getInstance(MemoryIntrospector.class);
    final StorageConnectorProvider storageConnectorProvider = injector.getInstance(Key.get(StorageConnectorProvider.class, MultiStageQuery.class));
    final StorageConnector storageConnector = storageConnectorProvider.createStorageConnector(toolbox.getIndexingTmpDir());
    this.injector = injector.createChildInjector(
        binder -> binder.bind(Key.get(StorageConnector.class, MultiStageQuery.class))
                        .toInstance(storageConnector));
  }

  @Override
  public String queryId()
  {
    return task.getId();
  }

  @Override
  public ControllerQueryKernelConfig queryKernelConfig(final MSQSpec querySpec)
  {
    final ControllerMemoryParameters memoryParameters =
        ControllerMemoryParameters.createProductionInstance(
            memoryIntrospector,
            querySpec.getTuningConfig().getMaxNumWorkers(),
            MultiStageQueryContext.getFrameSize(querySpec.getContext())
        );

    final ControllerQueryKernelConfig config = makeQueryKernelConfig(querySpec, memoryParameters);

    log.debug(
        "Query[%s] using %s[%s], %s[%s], %s[%s].",
        queryId(),
        MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE,
        config.isDurableStorage(),
        MultiStageQueryContext.CTX_FAULT_TOLERANCE,
        config.isFaultTolerant(),
        MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES,
        config.getMaxConcurrentStages()
    );

    return config;
  }

  @Override
  public void emitMetric(MSQMetriceEventBuilder metricBuilder)
  {
    // Attach task specific dimensions
    metricBuilder.setTaskDimensions(task, taskQuerySpecContext);
    toolbox.getEmitter().emit(metricBuilder);
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return toolbox.getJsonMapper();
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public DruidNode selfNode()
  {
    return injector.getInstance(Key.get(DruidNode.class, Self.class));
  }

  @Override
  public InputSpecSlicer newTableInputSpecSlicer(final WorkerManager workerManager)
  {
    final SegmentSource includeSegmentSource =
        MultiStageQueryContext.getSegmentSources(taskQuerySpecContext, DEFAULT_SEGMENT_SOURCE);
    return new IndexerTableInputSpecSlicer(
        toolbox.getCoordinatorClient(),
        toolbox.getTaskActionClient(),
        includeSegmentSource
    );
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    return toolbox.getTaskActionClient();
  }

  @Override
  public TaskLockType taskLockType()
  {
    return taskLockType;
  }

  @Override
  public WorkerClient newWorkerClient()
  {
    return new IndexerWorkerClient(clientFactory, overlordClient, jsonMapper());
  }

  @Override
  public void registerController(Controller controller, final Closer closer)
  {
    ChatHandler chatHandler = new ControllerChatHandler(
        controller,
        taskDataSource,
        toolbox.getAuthorizerMapper()
    );
    toolbox.getChatHandlerProvider().register(controller.queryId(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(controller.queryId()));
  }

  @Override
  public WorkerManager newWorkerManager(
      final String queryId,
      final MSQSpec querySpec,
      final ControllerQueryKernelConfig queryKernelConfig,
      final WorkerFailureListener workerFailureListener
  )
  {
    return new MSQWorkerTaskLauncher(
        queryId,
        taskDataSource,
        overlordClient,
        workerFailureListener,
        makeTaskContext(querySpec, queryKernelConfig, taskContext),
        // 10 minutes +- 2 minutes jitter
        TimeUnit.SECONDS.toMillis(600 + ThreadLocalRandom.current().nextInt(-4, 5) * 30L),
        new MSQWorkerTaskLauncherConfig()
    );
  }

  @Override
  public File taskTempDir()
  {
    return toolbox.getIndexingTmpDir();
  }

  /**
   * Helper method for {@link #queryKernelConfig(MSQSpec)}. Also used in tests.
   */
  public static ControllerQueryKernelConfig makeQueryKernelConfig(
      final MSQSpec querySpec,
      final ControllerMemoryParameters memoryParameters
  )
  {
    final QueryContext queryContext = querySpec.getContext();
    final int maxConcurrentStages =
        MultiStageQueryContext.getMaxConcurrentStagesWithDefault(queryContext, DEFAULT_MAX_CONCURRENT_STAGES);
    final boolean isFaultToleranceEnabled = MultiStageQueryContext.isFaultToleranceEnabled(queryContext);
    final boolean isDurableStorageEnabled;

    if (isFaultToleranceEnabled) {
      if (!queryContext.containsKey(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE)) {
        // if context key not set, enable durableStorage automatically.
        isDurableStorageEnabled = true;
      } else {
        // if context key is set, and durableStorage is turned on.
        if (MultiStageQueryContext.isDurableStorageEnabled(queryContext)) {
          isDurableStorageEnabled = true;
        } else {
          throw new MSQException(
              UnknownFault.forMessage(
                  StringUtils.format(
                      "Context param[%s] cannot be explicitly set to false when context param[%s] is"
                      + " set to true. Either remove the context param[%s] or explicitly set it to true.",
                      MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE,
                      MultiStageQueryContext.CTX_FAULT_TOLERANCE,
                      MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE
                  )
              )
          );
        }
      }
    } else {
      isDurableStorageEnabled = MultiStageQueryContext.isDurableStorageEnabled(queryContext);
    }

    return ControllerQueryKernelConfig
        .builder()
        .pipeline(maxConcurrentStages > 1)
        .durableStorage(isDurableStorageEnabled)
        .faultTolerance(isFaultToleranceEnabled)
        .destination(querySpec.getDestination())
        .maxConcurrentStages(maxConcurrentStages)
        .maxRetainedPartitionSketchBytes(memoryParameters.getPartitionStatisticsMaxRetainedBytes())
        .workerContextMap(makeWorkerContextMap(querySpec, isDurableStorageEnabled, maxConcurrentStages))
        .build();
  }

  /**
   * Helper method for {@link #makeQueryKernelConfig} and {@link #makeTaskContext}. Makes the worker context map,
   * i.e., the map that will become {@link WorkOrder#getWorkerContext()}.
   */
  public static Map<String, Object> makeWorkerContextMap(
      final MSQSpec querySpec,
      final boolean durableStorageEnabled,
      final int maxConcurrentStages
  )
  {
    final QueryContext queryContext = querySpec.getContext();
    final long maxParseExceptions = MultiStageQueryContext.getMaxParseExceptions(queryContext);
    final FrameType rowBasedFrameType = MultiStageQueryContext.getRowBasedFrameType(queryContext);
    final boolean removeNullBytes = MultiStageQueryContext.removeNullBytes(queryContext);
    final boolean includeAllCounters = MultiStageQueryContext.getIncludeAllCounters(queryContext);
    final boolean isReindex = MultiStageQueryContext.isReindex(queryContext);
    final int frameSize = MultiStageQueryContext.getFrameSize(queryContext);
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

    builder
        .put(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE, durableStorageEnabled)
        .put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, maxParseExceptions)
        .put(MultiStageQueryContext.CTX_IS_REINDEX, isReindex)
        .put(MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, maxConcurrentStages)
        .put(MultiStageQueryContext.CTX_ROW_BASED_FRAME_TYPE, (int) rowBasedFrameType.version())
        .put(MultiStageQueryContext.CTX_REMOVE_NULL_BYTES, removeNullBytes)
        .put(MultiStageQueryContext.CTX_INCLUDE_ALL_COUNTERS, includeAllCounters)
        .put(MultiStageQueryContext.CTX_MAX_FRAME_SIZE, frameSize);

    if (querySpec.getId() != null) {
      builder.put(BaseQuery.QUERY_ID, querySpec.getId());
    }

    if (queryContext.containsKey(QueryContexts.CTX_SQL_QUERY_ID)) {
      builder.put(BaseQuery.SQL_QUERY_ID, queryContext.get(QueryContexts.CTX_SQL_QUERY_ID));
    }

    MSQDestination destination = querySpec.getDestination();
    if (destination.toSelectDestination() != null) {
      builder.put(
          MultiStageQueryContext.CTX_SELECT_DESTINATION,
          destination.toSelectDestination().getName()
      );
    }

    return builder.build();
  }

  /**
   * Helper method for {@link ControllerContext#newWorkerManager}, split out to be used in tests.
   *
   * @param querySpec MSQ query spec; used for
   */
  public static Map<String, Object> makeTaskContext(
      final MSQSpec querySpec,
      final ControllerQueryKernelConfig queryKernelConfig,
      final Map<String, Object> controllerTaskContext
  )
  {
    final ImmutableMap.Builder<String, Object> taskContextOverridesBuilder = ImmutableMap.builder();

    // Put worker context into the task context. That way, workers can get these context keys either from
    // WorkOrder#getContext or Task#getContext.
    taskContextOverridesBuilder.putAll(
        makeWorkerContextMap(
            querySpec,
            queryKernelConfig.isDurableStorage(),
            queryKernelConfig.getMaxConcurrentStages()
        )
    );

    // Put the lookup loading info in the task context to facilitate selective loading of lookups.
    if (controllerTaskContext.get(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE) != null) {
      taskContextOverridesBuilder.put(
          LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE,
          controllerTaskContext.get(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE)
      );
    }
    if (controllerTaskContext.get(LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD) != null) {
      taskContextOverridesBuilder.put(
          LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD,
          controllerTaskContext.get(LookupLoadingSpec.CTX_LOOKUPS_TO_LOAD)
      );
    }

    // propagate the controller's tags to the worker task for enhanced metrics reporting
    @SuppressWarnings("unchecked")
    Map<String, Object> tags = (Map<String, Object>) controllerTaskContext.get(DruidMetrics.TAGS);
    if (tags != null) {
      taskContextOverridesBuilder.put(DruidMetrics.TAGS, tags);
    }

    return taskContextOverridesBuilder.build();
  }
}
