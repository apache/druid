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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.util.Map;


public abstract class SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity>
    extends AbstractTask implements ChatHandler
{
  public static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;
  private static final EmittingLogger log = new EmittingLogger(SeekableStreamIndexTask.class);

  protected final DataSchema dataSchema;
  protected final SeekableStreamIndexTaskTuningConfig tuningConfig;
  protected final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig;
  protected final Map<String, Object> context;
  protected final LockGranularity lockGranularityToUse;
  protected final TaskLockType lockTypeToUse;

  // Lazily initialized, to avoid calling it on the overlord when tasks are instantiated.
  // See https://github.com/apache/druid/issues/7724 for issues that can cause.
  // By the way, lazily init is synchronized because the runner may be needed in multiple threads.
  private final Supplier<SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType, ?>> runnerSupplier;

  @MonotonicNonNull
  protected AuthorizerMapper authorizerMapper;

  public SeekableStreamIndexTask(
      final String id,
      @Nullable final TaskResource taskResource,
      final DataSchema dataSchema,
      final SeekableStreamIndexTaskTuningConfig tuningConfig,
      final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig,
      @Nullable final Map<String, Object> context,
      @Nullable final String groupId
  )
  {
    super(
        id,
        groupId,
        taskResource,
        dataSchema.getDataSource(),
        context,
        IngestionMode.APPEND
    );
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.context = context;
    this.runnerSupplier = Suppliers.memoize(this::createTaskRunner);
    this.lockGranularityToUse = getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK)
                                ? LockGranularity.TIME_CHUNK
                                : LockGranularity.SEGMENT;
    this.lockTypeToUse = TaskLocks.determineLockTypeForAppend(getContext());
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
  public TaskStatus runTask(final TaskToolbox toolbox)
  {
    emitMetric(toolbox.getEmitter(), "ingest/count", 1);
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

  public Appenderator newAppenderator(
      TaskToolbox toolbox,
      FireDepartmentMetrics metrics,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    return toolbox.getAppenderatorsManager().createRealtimeAppenderatorForTask(
        toolbox.getSegmentLoaderConfig(),
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
        toolbox.getQueryProcessingPool(),
        toolbox.getJoinableFactory(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getCachePopulatorStats(),
        rowIngestionMeters,
        parseExceptionHandler,
        isUseMaxMemoryEstimates(),
        toolbox.getCentralizedTableSchemaConfig()
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
                lockGranularityToUse,
                lockTypeToUse
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getJsonMapper(),
        metrics
    );
  }

  public boolean withinMinMaxRecordTime(final InputRow row)
  {
    final boolean beforeMinimumMessageTime = ioConfig.getMinimumMessageTime().isPresent()
                                             && ioConfig.getMinimumMessageTime().get().isAfter(row.getTimestamp());

    final boolean afterMaximumMessageTime = ioConfig.getMaximumMessageTime().isPresent()
                                            && ioConfig.getMaximumMessageTime().get().isBefore(row.getTimestamp());

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

  protected abstract SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType, RecordType> createTaskRunner();

  /**
   * Deprecated method for providing the {@link RecordSupplier} that connects with the stream. New extensions should
   * override {@link #newTaskRecordSupplier(TaskToolbox)} instead.
   */
  @Deprecated
  protected RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> newTaskRecordSupplier()
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Subclasses must override this method to provide the {@link RecordSupplier} that connects with the stream.
   *
   * The default implementation delegates to {@link #newTaskRecordSupplier()}, which is deprecated, in order to support
   * existing extensions that have implemented that older method instead of this newer one. New extensions should
   * override this method, not {@link #newTaskRecordSupplier()}.
   */
  protected RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> newTaskRecordSupplier(final TaskToolbox toolbox)
  {
    return newTaskRecordSupplier();
  }

  @VisibleForTesting
  public Appenderator getAppenderator()
  {
    return getRunner().getAppenderator();
  }

  @VisibleForTesting
  public SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType, ?> getRunner()
  {
    return runnerSupplier.get();
  }
}
