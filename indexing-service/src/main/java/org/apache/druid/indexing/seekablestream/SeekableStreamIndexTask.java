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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.data.input.InputRow;
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
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.utils.RandomIdUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.utils.CircularBuffer;

import javax.annotation.Nullable;
import java.util.Map;


public abstract class SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType>
    extends AbstractTask implements ChatHandler
{
  public static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamIndexTask.class);

  protected final DataSchema dataSchema;
  protected final SeekableStreamIndexTaskTuningConfig tuningConfig;
  protected final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig;
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
  private final Supplier<SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType>> runnerSupplier;

  public SeekableStreamIndexTask(
      final String id,
      @Nullable final TaskResource taskResource,
      final DataSchema dataSchema,
      final SeekableStreamIndexTaskTuningConfig tuningConfig,
      final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig,
      @Nullable final Map<String, Object> context,
      @Nullable final ChatHandlerProvider chatHandlerProvider,
      final AuthorizerMapper authorizerMapper,
      final RowIngestionMetersFactory rowIngestionMetersFactory,
      @Nullable final String groupId,
      AppenderatorsManager appenderatorsManager
  )
  {
    super(
        id,
        groupId,
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

  private static String makeTaskId(String dataSource, String type)
  {
    final String suffix = RandomIdUtils.getRandomId();
    return Joiner.on("_").join(type, dataSource, suffix);
  }

  protected static String getFormattedId(String dataSource, String type)
  {
    return makeTaskId(dataSource, type);
  }

  protected static String getFormattedGroupId(String dataSource, String type)
  {
    return StringUtils.format("%s_%s", type, dataSource);
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_REALTIME_TASK_PRIORITY);
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public SeekableStreamIndexTaskTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("ioConfig")
  public SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> getIOConfig()
  {
    return ioConfig;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox)
  {
    return getRunner().run(toolbox);
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    if (taskConfig.isRestoreTasksOnRestart()) {
      getRunner().stopGracefully();
    } else {
      getRunner().stopForcefully();
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (getRunner().getAppenderator() == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return (queryPlus, responseContext) -> queryPlus.run(getRunner().getAppenderator(), responseContext);
  }

  public Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    return appenderatorsManager.createRealtimeAppenderatorForTask(
        getId(),
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
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
                NumberedShardSpecFactory.instance(),
                lockGranularityToUse
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getObjectMapper(),
        metrics
    );
  }

  public boolean withinMinMaxRecordTime(final InputRow row)
  {
    final boolean beforeMinimumMessageTime = ioConfig.getMinimumMessageTime().isPresent()
                                             && ioConfig.getMinimumMessageTime().get().isAfter(row.getTimestamp());

    final boolean afterMaximumMessageTime = ioConfig.getMaximumMessageTime().isPresent()
                                            && ioConfig.getMaximumMessageTime().get().isBefore(row.getTimestamp());

    if (!Intervals.ETERNITY.contains(row.getTimestamp())) {
      final String errorMsg = StringUtils.format(
          "Encountered row with timestamp that cannot be represented as a long: [%s]",
          row
      );
      throw new ParseException(errorMsg);
    }

    if (log.isDebugEnabled()) {
      if (beforeMinimumMessageTime) {
        log.debug(
            "CurrentTimeStamp[%s] is before MinimumMessageTime[%s]",
            row.getTimestamp(),
            ioConfig.getMinimumMessageTime().get()
        );
      } else if (afterMaximumMessageTime) {
        log.debug(
            "CurrentTimeStamp[%s] is after MaximumMessageTime[%s]",
            row.getTimestamp(),
            ioConfig.getMaximumMessageTime().get()
        );
      }
    }

    return !beforeMinimumMessageTime && !afterMaximumMessageTime;
  }

  protected abstract SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType> createTaskRunner();

  protected abstract RecordSupplier<PartitionIdType, SequenceOffsetType> newTaskRecordSupplier();

  @VisibleForTesting
  public Appenderator getAppenderator()
  {
    return getRunner().getAppenderator();
  }

  @VisibleForTesting
  public SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType> getRunner()
  {
    return runnerSupplier.get();
  }
}
