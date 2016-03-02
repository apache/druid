/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.RetryUtils;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.query.DruidMetrics;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.timeline.DataSegment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

public class KafkaIndexTask extends AbstractTask
{
  private static final Logger log = new Logger(KafkaIndexTask.class);
  private static final String TYPE = "index_kafka";
  private static final Random RANDOM = new Random();
  private static final long POLL_TIMEOUT = 100;
  private static final String METADATA_NEXT_PARTITIONS = "nextPartitions";

  private final DataSchema dataSchema;
  private final InputRowParser<ByteBuffer> parser;
  private final KafkaTuningConfig tuningConfig;
  private final KafkaIOConfig ioConfig;

  private volatile Appenderator appenderator = null;
  private volatile FireDepartmentMetrics fireDepartmentMetrics = null;
  private volatile boolean startedReading = false;
  private volatile boolean stopping = false;
  private volatile boolean publishing = false;
  private volatile Thread runThread = null;

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), RANDOM.nextInt()) : id,
        String.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.parser = Preconditions.checkNotNull((InputRowParser<ByteBuffer>) dataSchema.getParser(), "parser");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
  }

  private static String makeTaskId(String dataSource, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return Joiner.on("_").join(TYPE, dataSource, suffix);
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

  /**
   * Public for tests.
   */
  @JsonIgnore
  public boolean hasStartedReading()
  {
    return startedReading;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema,
        new RealtimeIOConfig(null, null, null),
        null
    );
    fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler().addMonitor(
        new RealtimeMetricsMonitor(
            ImmutableList.of(fireDepartmentForMetrics),
            ImmutableMap.of(DruidMetrics.TASK_ID, new String[]{getId()})
        )
    );

    try (
        final Appenderator appenderator0 = newAppenderator(fireDepartmentMetrics, toolbox);
        final FiniteAppenderatorDriver driver = newDriver(appenderator0, toolbox);
        final KafkaConsumer<byte[], byte[]> consumer = newConsumer()
    ) {
      appenderator = appenderator0;

      final String topic = ioConfig.getStartPartitions().getTopic();

      // Start up, set up initial offsets.
      final Object restoredMetadata = driver.startJob();
      final Map<Integer, Long> nextOffsets = Maps.newHashMap();
      if (restoredMetadata == null) {
        nextOffsets.putAll(ioConfig.getStartPartitions().getPartitionOffsetMap());
      } else {
        final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
        final KafkaPartitions restoredNextPartitions = toolbox.getObjectMapper().convertValue(
            restoredMetadataMap.get(METADATA_NEXT_PARTITIONS),
            KafkaPartitions.class
        );
        nextOffsets.putAll(restoredNextPartitions.getPartitionOffsetMap());

        // Sanity checks.
        if (!restoredNextPartitions.getTopic().equals(ioConfig.getStartPartitions().getTopic())) {
          throw new ISE(
              "WTF?! Restored topic[%s] but expected topic[%s]",
              restoredNextPartitions.getTopic(),
              ioConfig.getStartPartitions().getTopic()
          );
        }

        if (!nextOffsets.keySet().equals(ioConfig.getStartPartitions().getPartitionOffsetMap().keySet())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              nextOffsets.keySet(),
              ioConfig.getStartPartitions().getPartitionOffsetMap().keySet()
          );
        }
      }

      // Set up committer.
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Map<Integer, Long> snapshot = ImmutableMap.copyOf(nextOffsets);

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return ImmutableMap.of(
                  METADATA_NEXT_PARTITIONS, new KafkaPartitions(
                      ioConfig.getStartPartitions().getTopic(),
                      snapshot
                  )
              );
            }

            @Override
            public void run()
            {
              // Do nothing.
            }
          };
        }
      };

      // Initialize consumer assignment.
      final Set<Integer> assignment = Sets.newHashSet();
      for (Map.Entry<Integer, Long> entry : nextOffsets.entrySet()) {
        final long endOffset = ioConfig.getEndPartitions().getPartitionOffsetMap().get(entry.getKey());
        if (entry.getValue() < endOffset) {
          assignment.add(entry.getKey());
        } else if (entry.getValue() == endOffset) {
          log.info("Finished reading partition[%d].", entry.getKey());
        } else {
          throw new ISE(
              "WTF?! Cannot start from offset[%,d] > endOffset[%,d]",
              entry.getValue(),
              endOffset
          );
        }
      }

      assignPartitions(consumer, topic, assignment);

      // Seek to starting offsets.
      for (final int partition : assignment) {
        final long offset = nextOffsets.get(partition);
        log.info("Seeking partition[%d] to offset[%,d].", partition, offset);
        consumer.seek(new TopicPartition(topic, partition), offset);
      }

      // Main loop.
      // Could eventually support early termination (triggered by a supervisor)
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = true;
      while (stillReading) {
        if (stopping) {
          log.info("Stopping early.");
          break;
        }

        // The retrying business is because the KafkaConsumer throws OffsetOutOfRangeException if the seeked-to
        // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
        // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
        final ConsumerRecords<byte[], byte[]> records = RetryUtils.retry(
            new Callable<ConsumerRecords<byte[], byte[]>>()
            {
              @Override
              public ConsumerRecords<byte[], byte[]> call() throws Exception
              {
                try {
                  return consumer.poll(POLL_TIMEOUT);
                }
                finally {
                  startedReading = true;
                }
              }
            },
            new Predicate<Throwable>()
            {
              @Override
              public boolean apply(Throwable input)
              {
                return input instanceof OffsetOutOfRangeException;
              }
            },
            Integer.MAX_VALUE
        );

        for (ConsumerRecord<byte[], byte[]> record : records) {
          if (log.isTraceEnabled()) {
            log.trace(
                "Got topic[%s] partition[%d] offset[%,d].",
                record.topic(),
                record.partition(),
                record.offset()
            );
          }

          if (record.offset() < ioConfig.getEndPartitions().getPartitionOffsetMap().get(record.partition())) {
            if (record.offset() != nextOffsets.get(record.partition())) {
              throw new ISE(
                  "WTF?! Got offset[%,d] after offset[%,d] in partition[%d].",
                  record.offset(),
                  nextOffsets.get(record.partition()),
                  record.partition()
              );
            }

            try {
              final InputRow row = Preconditions.checkNotNull(parser.parse(ByteBuffer.wrap(record.value())), "row");
              final SegmentIdentifier identifier = driver.add(row, committerSupplier);

              if (identifier == null) {
                // Failure to allocate segment puts determinism at risk, bail out to be safe.
                // May want configurable behavior here at some point.
                // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
                throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
              }

              fireDepartmentMetrics.incrementProcessed();
            }
            catch (ParseException e) {
              if (tuningConfig.isReportParseExceptions()) {
                throw e;
              } else {
                log.debug(
                    e,
                    "Dropping unparseable row from partition[%d] offset[%,d].",
                    record.partition(),
                    record.offset()
                );

                fireDepartmentMetrics.incrementUnparseable();
              }
            }

            final long nextOffset = record.offset() + 1;
            final long endOffset = ioConfig.getEndPartitions().getPartitionOffsetMap().get(record.partition());

            nextOffsets.put(record.partition(), nextOffset);

            if (nextOffset == endOffset && assignment.remove(record.partition())) {
              log.info("Finished reading topic[%s], partition[%,d].", record.topic(), record.partition());
              assignPartitions(consumer, topic, assignment);
              stillReading = !assignment.isEmpty();
            }
          }
        }
      }

      // Persist pending data.
      final Committer finalCommitter = committerSupplier.get();
      driver.persist(finalCommitter);

      publishing = true;
      if (stopping) {
        // Stopped gracefully. Exit code shouldn't matter, so fail to be on the safe side.
        return TaskStatus.failure(getId());
      }

      final TransactionalSegmentPublisher publisher = new TransactionalSegmentPublisher()
      {
        @Override
        public boolean publishSegments(Set<DataSegment> segments, Object commitMetadata) throws IOException
        {
          // Sanity check, we should only be publishing things that match our desired end state.
          if (!ioConfig.getEndPartitions().equals(((Map) commitMetadata).get(METADATA_NEXT_PARTITIONS))) {
            throw new ISE("WTF?! Driver attempted to publish invalid metadata[%s].", commitMetadata);
          }

          final SegmentInsertAction action;

          if (ioConfig.isUseTransaction()) {
            action = new SegmentInsertAction(
                segments,
                new KafkaDataSourceMetadata(ioConfig.getStartPartitions()),
                new KafkaDataSourceMetadata(ioConfig.getEndPartitions())
            );
          } else {
            action = new SegmentInsertAction(segments, null, null);
          }

          log.info("Publishing with isTransaction[%s].", ioConfig.isUseTransaction());

          return toolbox.getTaskActionClient().submit(action).isSuccess();
        }
      };

      final SegmentsAndMetadata published = driver.finish(publisher, committerSupplier.get());
      if (published == null) {
        throw new ISE("Transaction failure publishing segments, aborting");
      } else {
        log.info(
            "Published segments[%s] with metadata[%s].",
            Joiner.on(", ").join(
                Iterables.transform(
                    published.getSegments(),
                    new Function<DataSegment, String>()
                    {
                      @Override
                      public String apply(DataSegment input)
                      {
                        return input.getIdentifier();
                      }
                    }
                )
            ),
            published.getCommitMetadata()
        );
      }
    }

    return success();
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @Override
  public void stopGracefully()
  {
    log.info("Stopping gracefully.");

    stopping = true;
    if (publishing && runThread.isAlive()) {
      log.info("stopGracefully: Run thread started publishing, interrupting it.");
      runThread.interrupt();
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
      {
        return query.run(appenderator, responseContext);
      }
    };
  }

  @VisibleForTesting
  public FireDepartmentMetrics getFireDepartmentMetrics()
  {
    return fireDepartmentMetrics;
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    return Appenderators.createRealtime(
        dataSchema,
        tuningConfig.withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist")),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        tuningConfig.getBuildV9Directly() ? toolbox.getIndexMergerV9() : toolbox.getIndexMerger(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
        toolbox.getCache(),
        toolbox.getCacheConfig()
    );
  }

  private FiniteAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox
  )
  {
    return new FiniteAppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(
            toolbox.getTaskActionClient(),
            dataSchema,
            ioConfig.getSequenceName()
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        tuningConfig.getMaxRowsPerSegment(),
        tuningConfig.getHandoffConditionTimeout()
    );
  }

  private KafkaConsumer<byte[], byte[]> newConsumer()
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

  private static void assignPartitions(
      final KafkaConsumer consumer,
      final String topic,
      final Set<Integer> partitions
  )
  {
    consumer.assign(
        Lists.newArrayList(
            Iterables.transform(
                partitions,
                new Function<Integer, TopicPartition>()
                {
                  @Override
                  public TopicPartition apply(Integer n)
                  {
                    return new TopicPartition(topic, n);
                  }
                }
            )
        )
    );
  }
}
