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

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.stats.RowIngestionMetersFactory;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.common.task.Tasks;
import io.druid.indexing.kafka.supervisor.KafkaSupervisor;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.server.security.AuthorizerMapper;
import io.druid.utils.CircularBuffer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaIndexTask extends AbstractTask implements ChatHandler
{
  public enum Status
  {
    NOT_STARTED,
    STARTING,
    READING,
    PAUSED,
    PUBLISHING
    // ideally this should be called FINISHING now as the task does incremental publishes
    // through out its lifetime
  }

  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTask.class);
  private static final String TYPE = "index_kafka";
  private static final Random RANDOM = new Random();
  static final long POLL_TIMEOUT = 100;
  static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;

  private final DataSchema dataSchema;
  private final InputRowParser<ByteBuffer> parser;
  private final KafkaTuningConfig tuningConfig;
  private final KafkaIOConfig ioConfig;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final KafkaIndexTaskRunner runner;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), RANDOM.nextInt()) : id,
        StringUtils.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.parser = Preconditions.checkNotNull((InputRowParser<ByteBuffer>) dataSchema.getParser(), "parser");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    final CircularBuffer<Throwable> savedParseExceptions;
    if (tuningConfig.getMaxSavedParseExceptions() > 0) {
      savedParseExceptions = new CircularBuffer<>(tuningConfig.getMaxSavedParseExceptions());
    } else {
      savedParseExceptions = null;
    }

    if (context != null && context.get(KafkaSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED) != null
        && ((boolean) context.get(KafkaSupervisor.IS_INCREMENTAL_HANDOFF_SUPPORTED))) {
      runner = new IncrementalPublishingKafkaIndexTaskRunner(
          this,
          parser,
          authorizerMapper,
          this.chatHandlerProvider,
          savedParseExceptions,
          rowIngestionMetersFactory
      );
    } else {
      runner = new LegacyKafkaIndexTaskRunner(
          this,
          parser,
          authorizerMapper,
          this.chatHandlerProvider,
          savedParseExceptions,
          rowIngestionMetersFactory
      );
    }
  }

  long getPollRetryMs()
  {
    return pollRetryMs;
  }

  private static String makeTaskId(String dataSource, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return Joiner.on("_").join(TYPE, dataSource, suffix);
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_REALTIME_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return TYPE;
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
  public KafkaTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("ioConfig")
  public KafkaIOConfig getIOConfig()
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

  Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
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

  StreamAppenderatorDriver newDriver(
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

  KafkaConsumer<byte[], byte[]> newConsumer()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Properties props = new Properties();

      for (Map.Entry<String, String> entry : ioConfig.getConsumerProperties().entrySet()) {
        props.setProperty(entry.getKey(), entry.getValue());
      }

      props.setProperty("enable.auto.commit", "false");
      props.setProperty("auto.offset.reset", "none");
      props.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
      props.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());

      return new KafkaConsumer<>(props);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  static void assignPartitions(
      final KafkaConsumer consumer,
      final String topic,
      final Set<Integer> partitions
  )
  {
    consumer.assign(
        Lists.newArrayList(
            partitions.stream().map(n -> new TopicPartition(topic, n)).collect(Collectors.toList())
        )
    );
  }

  boolean withinMinMaxRecordTime(final InputRow row)
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

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @VisibleForTesting
  Appenderator getAppenderator()
  {
    return runner.getAppenderator();
  }

  @VisibleForTesting
  KafkaIndexTaskRunner getRunner()
  {
    return runner;
  }
}
