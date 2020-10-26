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

package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.utils.CircularBuffer;

import java.util.Map;

public class PubsubIndexTask extends AbstractTask implements ChatHandler
{
  private static final Logger log = new Logger(PubsubIndexTask.class);

  private static final String TYPE = "index_pubsub";

  protected final DataSchema dataSchema;
  protected final PubsubIndexTaskTuningConfig tuningConfig;
  protected final Optional<ChatHandlerProvider> chatHandlerProvider;
  protected final Map<String, Object> context;
  protected final AuthorizerMapper authorizerMapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;
  protected final CircularBuffer<Throwable> savedParseExceptions;
  protected final AppenderatorsManager appenderatorsManager;
  protected final LockGranularity lockGranularityToUse;

  // Lazily initialized, to avoid calling it on the overlord when tasks are instantiated.
  // See https://github.com/apache/incubator-druid/issues/7724 for issues that can cause.
  // By the way, lazily init is synchronized because the runner may be needed in multiple threads.
  private final Supplier<PubsubIndexTaskRunner> runnerSupplier;

  private final PubsubIndexTaskIOConfig ioConfig;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public PubsubIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") PubsubIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") PubsubIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject AppenderatorsManager appenderatorsManager
  )
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        StringUtils.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    if (tuningConfig.getMaxSavedParseExceptions() > 0) {
      savedParseExceptions = new CircularBuffer<>(tuningConfig.getMaxSavedParseExceptions());
    } else {
      savedParseExceptions = null;
    }
    this.context = context;
    this.authorizerMapper = authorizerMapper;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.runnerSupplier = Suppliers.memoize(this::createTaskRunner);
    this.appenderatorsManager = appenderatorsManager;
    this.lockGranularityToUse = getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK)
                                ? LockGranularity.TIME_CHUNK
                                : LockGranularity.SEGMENT;
  }

  protected PubsubIndexTaskRunner createTaskRunner()
  {
    //noinspection unchecked
    return new PubsubIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory,
        appenderatorsManager,
        lockGranularityToUse
    );
  }

  protected PubsubRecordSupplier newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      return new PubsubRecordSupplier(ioConfig.getProjectId(), ioConfig.getSubscription());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @JsonProperty
  public PubsubIndexTaskTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("ioConfig")
  public PubsubIndexTaskIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    //TODO
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    return getRunner().run(toolbox);
  }

  public PubsubIndexTaskRunner getRunner()
  {
    return runnerSupplier.get();
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  public Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    return appenderatorsManager.createRealtimeAppenderatorForTask(
        getId(),
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getJsonMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
        toolbox.getJoinableFactory(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getCachePopulatorStats()
    );
  }


  public StreamAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final FireDepartmentMetrics metrics
  )
  {
    return new StreamAppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(
            toolbox.getTaskActionClient(),
            dataSchema,
            (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> new SegmentAllocateAction(
                schema.getDataSource(),
                row.getTimestamp(),
                schema.getGranularitySpec().getQueryGranularity(),
                schema.getGranularitySpec().getSegmentGranularity(),
                sequenceName,
                previousSegmentId,
                skipSegmentLineageCheck,
                NumberedPartialShardSpec.instance(),
                lockGranularityToUse
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getJsonMapper(),
        metrics
    );
  }
}
