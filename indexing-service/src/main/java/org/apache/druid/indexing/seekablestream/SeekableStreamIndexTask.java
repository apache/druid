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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
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
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.utils.CircularBuffer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


public abstract class SeekableStreamIndexTask<PartitionType, SequenceType> extends AbstractTask
    implements ChatHandler
{
  private final EmittingLogger log = new EmittingLogger(this.getClass());
  public static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;

  private static final Random RANDOM = ThreadLocalRandom.current();
  protected final DataSchema dataSchema;
  protected final InputRowParser<ByteBuffer> parser;
  protected final SeekableStreamTuningConfig tuningConfig;
  protected final SeekableStreamIOConfig<PartitionType, SequenceType> ioConfig;
  protected final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final SeekableStreamIndexTaskRunner<PartitionType, SequenceType> runner;
  protected final String type;
  protected final Map<String, Object> context;
  protected final AuthorizerMapper authorizerMapper;
  protected final RowIngestionMetersFactory rowIngestionMetersFactory;
  protected CircularBuffer<Throwable> savedParseExceptions;


  @JsonCreator
  public SeekableStreamIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") SeekableStreamTuningConfig tuningConfig,
      @JsonProperty("ioConfig") SeekableStreamIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      String type
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), RANDOM.nextInt(), type) : id,
        StringUtils.format("%s_%s", type, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );
    this.type = type;
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.parser = Preconditions.checkNotNull((InputRowParser<ByteBuffer>) dataSchema.getParser(), "parser");
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
    this.runner = createTaskRunner();
  }


  private static String makeTaskId(String dataSource, int randomBits, String type)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return Joiner.on("_").join(type, dataSource, suffix);
  }


  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_REALTIME_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return type;
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
  public SeekableStreamTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("ioConfig")
  public SeekableStreamIOConfig<PartitionType, SequenceType> getIOConfig()
  {
    return ioConfig;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox)
  {
    return runner.run(toolbox);
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @Override
  public void stopGracefully()
  {
    runner.stopGracefully();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (runner.getAppenderator() == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return (queryPlus, responseContext) -> queryPlus.run(runner.getAppenderator(), responseContext);
  }

  public Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    return Appenderators.createRealtime(
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
                skipSegmentLineageCheck
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getObjectMapper(),
        metrics
    );
  }

  protected abstract RecordSupplier<PartitionType, SequenceType> newRecordSupplier();

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

  protected abstract SeekableStreamIndexTaskRunner<PartitionType, SequenceType> createTaskRunner();

  @VisibleForTesting
  public Appenderator getAppenderator()
  {
    return runner.getAppenderator();
  }

  @VisibleForTesting
  public SeekableStreamIndexTaskRunner<PartitionType, SequenceType> getRunner()
  {
    return runner;
  }

}
