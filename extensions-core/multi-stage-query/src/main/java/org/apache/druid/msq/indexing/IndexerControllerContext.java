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
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerMemoryParameters;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerFailureListener;
import org.apache.druid.msq.exec.WorkerManager;
import org.apache.druid.msq.indexing.client.ControllerChatHandler;
import org.apache.druid.msq.indexing.client.IndexerWorkerClient;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.table.TableInputSpecSlicer;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContext;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.server.DruidNode;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Implementation for {@link ControllerContext} required to run multi-stage queries as indexing tasks.
 */
public class IndexerControllerContext implements ControllerContext
{
  private static final Logger log = new Logger(IndexerControllerContext.class);

  private final MSQControllerTask task;
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final ServiceClientFactory clientFactory;
  private final OverlordClient overlordClient;
  private final ServiceMetricEvent.Builder metricBuilder;

  public IndexerControllerContext(
      final MSQControllerTask task,
      final TaskToolbox toolbox,
      final Injector injector,
      final ServiceClientFactory clientFactory,
      final OverlordClient overlordClient
  )
  {
    this.task = task;
    this.toolbox = toolbox;
    this.injector = injector;
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
    this.metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
  }

  @Override
  public ControllerQueryKernelConfig queryKernelConfig(
      final MSQSpec querySpec,
      final QueryDefinition queryDef
  )
  {
    final MemoryIntrospector memoryIntrospector = injector.getInstance(MemoryIntrospector.class);
    final ControllerMemoryParameters memoryParameters =
        ControllerMemoryParameters.createProductionInstance(
            memoryIntrospector,
            queryDef.getFinalStageDefinition().getMaxWorkerCount()
        );

    final ControllerQueryKernelConfig config = makeQueryKernelConfig(querySpec, memoryParameters);

    log.debug(
        "Query[%s] using %s[%s], %s[%s], %s[%s].",
        queryDef.getQueryId(),
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
  public void emitMetric(String metric, Number value)
  {
    toolbox.getEmitter().emit(metricBuilder.setMetric(metric, value));
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
  public InputSpecSlicer newTableInputSpecSlicer()
  {
    final SegmentSource includeSegmentSource =
        MultiStageQueryContext.getSegmentSources(task.getQuerySpec().getQuery().context());
    return new TableInputSpecSlicer(
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
  public WorkerClient newWorkerClient()
  {
    return new IndexerWorkerClient(clientFactory, overlordClient, jsonMapper());
  }

  @Override
  public void registerController(Controller controller, final Closer closer)
  {
    ChatHandler chatHandler = new ControllerChatHandler(
        controller,
        task.getDataSource(),
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
        task.getDataSource(),
        overlordClient,
        workerFailureListener,
        makeTaskContext(querySpec, queryKernelConfig, task.getContext()),
        // 10 minutes +- 2 minutes jitter
        TimeUnit.SECONDS.toMillis(600 + ThreadLocalRandom.current().nextInt(-4, 5) * 30L)
    );
  }

  /**
   * Helper method for {@link #queryKernelConfig(MSQSpec, QueryDefinition)}. Also used in tests.
   */
  public static ControllerQueryKernelConfig makeQueryKernelConfig(
      final MSQSpec querySpec,
      final ControllerMemoryParameters memoryParameters
  )
  {
    final QueryContext queryContext = querySpec.getQuery().context();
    final int maxConcurrentStages = MultiStageQueryContext.getMaxConcurrentStages(queryContext);
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
        .build();
  }

  /**
   * Helper method for {@link #newWorkerManager}, split out to be used in tests.
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
    final long maxParseExceptions = MultiStageQueryContext.getMaxParseExceptions(querySpec.getQuery().context());

    taskContextOverridesBuilder
        .put(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE, queryKernelConfig.isDurableStorage())
        .put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, maxParseExceptions)
        .put(MultiStageQueryContext.CTX_IS_REINDEX, MSQControllerTask.isReplaceInputDataSourceTask(querySpec))
        .put(MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, queryKernelConfig.getMaxConcurrentStages());

    if (querySpec.getDestination().toSelectDestination() != null) {
      taskContextOverridesBuilder.put(
          MultiStageQueryContext.CTX_SELECT_DESTINATION,
          querySpec.getDestination().toSelectDestination().getName()
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
