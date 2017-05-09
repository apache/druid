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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import io.druid.indexing.common.actions.ResetDataSourceMetadataAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.DruidMetrics;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.timeline.DataSegment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.DateTime;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaIndexTask extends AbstractTask implements ChatHandler
{
  public static final long PAUSE_FOREVER = -1L;

  public enum Status
  {
    NOT_STARTED,
    STARTING,
    READING,
    PAUSED,
    PUBLISHING
  }

  private static final EmittingLogger log = new EmittingLogger(KafkaIndexTask.class);
  private static final String TYPE = "index_kafka";
  private static final Random RANDOM = new Random();
  private static final long POLL_TIMEOUT = 100;
  private static final long POLL_RETRY_MS = 30000;
  private static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;
  static final String METADATA_NEXT_PARTITIONS = "nextPartitions";

  private final DataSchema dataSchema;
  private final InputRowParser<ByteBuffer> parser;
  private final KafkaTuningConfig tuningConfig;
  private final KafkaIOConfig ioConfig;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;

  private final Map<Integer, Long> endOffsets = new ConcurrentHashMap<>();
  private final Map<Integer, Long> nextOffsets = new ConcurrentHashMap<>();
  private final Map<Integer, Long> maxEndOffsets = new HashMap<>();

  private TaskToolbox toolbox;

  private volatile FireDepartmentMetrics fireDepartmentMetrics = null;
  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile Thread runThread = null;

  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean publishOnStop = new AtomicBoolean(false);
  private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>(null);

  // The pause lock and associated conditions are to support coordination between the Jetty threads and the main
  // ingestion loop. The goal is to provide callers of the API a guarantee that if pause() returns successfully
  // the ingestion loop has been stopped at the returned offsets and will not ingest any more data until resumed. The
  // fields are used as follows (every step requires acquiring [pauseLock]):
  //   Pausing:
  //   - In pause(), [pauseRequested] is set to true and then execution waits for [status] to change to PAUSED, with the
  //     condition checked when [hasPaused] is signalled.
  //   - In possiblyPause() called from the main loop, if [pauseRequested] is true, [status] is set to PAUSED,
  //     [hasPaused] is signalled, and execution pauses until [pauseRequested] becomes false, either by being set or by
  //     the [pauseMillis] timeout elapsing. [pauseRequested] is checked when [shouldResume] is signalled.
  //   Resuming:
  //   - In resume(), [pauseRequested] is set to false, [shouldResume] is signalled, and execution waits for [status] to
  //     change to something other than PAUSED, with the condition checked when [shouldResume] is signalled.
  //   - In possiblyPause(), when [shouldResume] is signalled, if [pauseRequested] has become false the pause loop ends,
  //     [status] is changed to STARTING and [shouldResume] is signalled.

  private final Lock pauseLock = new ReentrantLock();
  private final Condition hasPaused = pauseLock.newCondition();
  private final Condition shouldResume = pauseLock.newCondition();

  // [pollRetryLock] and [isAwaitingRetry] is used when the Kafka consumer returns an OffsetOutOfRangeException and we
  // pause polling from Kafka for POLL_RETRY_MS before trying again. This allows us to signal the sleeping thread and
  // resume the main run loop in the case of a pause or stop request from a Jetty thread.
  private final Lock pollRetryLock = new ReentrantLock();
  private final Condition isAwaitingRetry = pollRetryLock.newCondition();

  // [statusLock] is used to synchronize the Jetty thread calling stopGracefully() with the main run thread. It prevents
  // the main run thread from switching into a publishing state while the stopGracefully() thread thinks it's still in
  // a pre-publishing state. This is important because stopGracefully() will try to use the [stopRequested] flag to stop
  // the main thread where possible, but this flag is not honored once publishing has begun so in this case we must
  // interrupt the thread. The lock ensures that if the run thread is about to transition into publishing state, it
  // blocks until after stopGracefully() has set [stopRequested] and then does a final check on [stopRequested] before
  // transitioning to publishing state.
  private final Object statusLock = new Object();

  private volatile boolean pauseRequested = false;
  private volatile long pauseMillis = 0;

  volatile int nextDriverIndex = 0;
  // Reverse sorted list of DriverHolders i.e. the most recent driverHolder is at front and the oldest at last
  private final List<DriverHolder> driverHolders = new CopyOnWriteArrayList<>();
  private final Lock driversListLock = new ReentrantLock();

  private final BlockingDeque<DriverHolder> publishQueue = new LinkedBlockingDeque<>();
  private final BlockingDeque<DriverHolder> handOffQueue = new LinkedBlockingDeque<>();

  private final ListeningExecutorService persistExecService;
  private final ListeningExecutorService publishExecService;
  private final ListeningExecutorService handOffExecService;

  private File driversRestoreFile;

  private final CountDownLatch persistLatch = new CountDownLatch(1);
  private final CountDownLatch handOffLatch = new CountDownLatch(1);

  @JsonCreator
  public KafkaIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KafkaTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KafkaIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider
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
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);

    this.endOffsets.putAll(ioConfig.getEndPartitions().getPartitionOffsetMap());
    for (Integer partition : endOffsets.keySet()) {
      maxEndOffsets.put(partition, Long.MAX_VALUE);
    }
    this.persistExecService = MoreExecutors.listeningDecorator(Execs.newBlockingSingleThreaded("persist-%d", 0));
    this.publishExecService = MoreExecutors.listeningDecorator(Execs.newBlockingSingleThreaded("publish-driver-%d", 1));
    this.handOffExecService = MoreExecutors.listeningDecorator(Execs.newBlockingSingleThreaded(
        "handoff-checker-%d",
        1
    ));
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

  private void startExecutors()
  {
    // start publish executor service
    publishExecService.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (true) {
              DriverHolder driverHolder = null;
              try {
                driverHolder = publishQueue.take();

                Preconditions.checkState(
                    driverHolder.driverStatus == DriverHolder.DriverStatus.PERSISTED,
                    String.format(
                        "WTH?! driver trying to publish but not yet persisted, driver status: [%s]",
                        driverHolder.driverStatus
                    )
                );

                driverHolder.driverStatus = DriverHolder.DriverStatus.PUBLISHING;

                log.info("Publishing driver [%s]", driverHolder.getMetadata());

                final SegmentsAndMetadata result = driverHolder.finish(toolbox, ioConfig.isUseTransaction());

                if (result == null) {
                  if (driverHolder.getMetadata().getDriverIndex() == -1) {
                    // indicates all drivers are finished, ok to shutdown
                    log.info("All drivers have published segments to the metadata store");
                  } else {
                    throw new ISE(
                        "Transaction failure publishing segments for driver [%s]",
                        driverHolder.getMetadata()
                    );
                  }
                } else {
                  log.info(
                      "Published segments[%s] with metadata[%s].",
                      Joiner.on(", ").join(
                          Iterables.transform(
                              result.getSegments(),
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
                      result.getCommitMetadata()
                  );
                }

                driverHolder.driverStatus = DriverHolder.DriverStatus.PUBLISHED;

                handOffQueue.addLast(driverHolder);
              }
              catch (Throwable t) {
                if ((t instanceof InterruptedException || (t instanceof RejectedExecutionException
                                                           && t.getCause() instanceof InterruptedException))) {
                  if (stopRequested.get() || handOffLatch.getCount() == 0) {
                    // we are shutting down, ignore the interrupt
                    log.warn("Stopping publish thread as we are interrupted and shutting down");
                    break;
                  } else {
                    // enqueue back
                    if (driverHolder != null) {
                      log.error(
                          t,
                          "Error in publish thread, enqueueing driver [%d] back to publish queue",
                          driverHolder.getDriverIndex()
                      );
                      driverHolder.driverStatus = DriverHolder.DriverStatus.PERSISTED;
                      publishQueue.addFirst(driverHolder);
                    }
                    continue;
                  }
                }
                log.makeAlert(t, "Error in publish thread, dying").emit();
                throwableAtomicReference.set(t);
                handOffLatch.countDown();
                Throwables.propagate(t);
              }
            }
          }
        }
    );

    handOffExecService.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            while (true) {
              try {
                final DriverHolder driverHolder = handOffQueue.take();

                Preconditions.checkState(
                    driverHolder.driverStatus == DriverHolder.DriverStatus.PUBLISHED,
                    String.format(
                        "WTH?! cannot wait for hand off for not published driver [%d], status: [%s]",
                        driverHolder.getDriverIndex(),
                        driverHolder.driverStatus
                    )
                );

                log.info("Waiting for driver [%s] to hand off", driverHolder.getMetadata());
                try {
                  if (driverHolder.getDriverIndex() != -1) {
                    driverHolder.waitForHandOff();
                  }
                  log.info("Handoff complete for driver [%d]", driverHolder.getDriverIndex());
                }
                catch (InterruptedException t) {
                  if (stopRequested.get()) {
                    log.warn("Stopping handoff thread as we are interrupted and shutting down");
                    break;
                  } else {
                    // enqueue back
                    handOffQueue.addFirst(driverHolder);
                  }
                }
                finally {
                  driverHolder.close();
                  if (!driverHolders.remove(driverHolder)) {
                    log.warn("Unable to remove driver [%d], it was not in the drivers list", driverHolder.getDriverIndex());
                  } else {
                    try {
                      lockDriversList();
                      persistDriversList();
                      log.info("Driver [%s] removed from drivers list", driverHolder.getMetadata());
                    }
                    finally {
                      unlockDriversList();
                    }
                  }
                }
              }
              catch (Throwable t) {
                if (t instanceof InterruptedException && (stopRequested.get() || handOffLatch.getCount() == 0)) {
                  log.warn("Stopping handoff thread as we are interrupted and shutting down");
                  break;
                }
                log.makeAlert(t, "Error in handoff thread, dying").emit();
                throwableAtomicReference.set(t);
                handOffLatch.countDown();
                Throwables.propagate(t);
              }
            }
          }
        }
    );
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");
    startTime = DateTime.now();
    this.toolbox = toolbox;
    status = Status.STARTING;

    startExecutors();

    toolbox.getTaskWorkDir().mkdirs();

    if (chatHandlerProvider.isPresent()) {
      log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(getId(), this, false);
    } else {
      log.warn("No chat handler detected");
    }

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

    final KafkaConsumer<byte[], byte[]> consumer = newConsumer();

    try {
      final String topic = ioConfig.getStartPartitions().getTopic();

      restoreState(toolbox);

      Set<Integer> assignment = assignPartitionsAndSeekToNext(consumer, topic);

      // Main loop.
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = !assignment.isEmpty();
      status = Status.READING;

      try {
        while (stillReading) {
          checkAndMayBeThrowException();
          if (possiblyPause(assignment)) {
            // The partition assignments may have changed while paused by a call to setEndOffsets() so reassign
            // partitions upon resuming. This is safe even if the end offsets have not been modified.
            assignment = assignPartitionsAndSeekToNext(consumer, topic);

            if (assignment.isEmpty()) {
              log.info("All partitions have been fully read");
              publishOnStop.set(true);
              stopRequested.set(true);
            }
          }

          if (stopRequested.get()) {
            break;
          }

          // The retrying business is because the KafkaConsumer throws OffsetOutOfRangeException if the seeked-to
          // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
          // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
          ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
          try {
            records = consumer.poll(POLL_TIMEOUT);
          }
          catch (OffsetOutOfRangeException e) {
            log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
            possiblyResetOffsetsOrWait(e.offsetOutOfRangePartitions(), consumer, toolbox);
            stillReading = ioConfig.isPauseAfterRead() || !assignment.isEmpty();
          }

          for (ConsumerRecord<byte[], byte[]> record : records) {

            if (log.isTraceEnabled()) {
              log.trace(
                  "Got topic[%s] partition[%d] offset[%,d].",
                  record.topic(),
                  record.partition(),
                  record.offset()
              );
            }

            if (record.offset() < endOffsets.get(record.partition())) {
              if (record.offset() != nextOffsets.get(record.partition())) {
                throw new ISE(
                    "WTF?! Got offset[%,d] after offset[%,d] in partition[%d].",
                    record.offset(),
                    nextOffsets.get(record.partition()),
                    record.partition()
                );
              }

              DriverHolder driverHolder = null;
              try {
                // find a Driver to consume this record
                for (DriverHolder holder : driverHolders) {
                  if (holder.canHandle(record)) {
                    driverHolder = holder;
                    break;
                  }
                }
                if (driverHolder == null) {
                  throw new ISE(
                      "WTH?! Could not find a driver to handle record: partition [%d] offset [%d], current drivers list: [%s]",
                      record.partition(),
                      record.offset(),
                      driverHolders
                  );
                }

                final byte[] valueBytes = record.value();
                if (valueBytes == null) {
                  throw new ParseException("null value");
                }

                final InputRow row = Preconditions.checkNotNull(parser.parse(ByteBuffer.wrap(valueBytes)), "row");

                if (!ioConfig.getMinimumMessageTime().isPresent() ||
                    !ioConfig.getMinimumMessageTime().get().isAfter(row.getTimestamp())) {

                  driverHolder.add(row);
                  fireDepartmentMetrics.incrementProcessed();
                } else {
                  fireDepartmentMetrics.incrementThrownAway();
                }
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

              driverHolder.incrementNextOffsets(record);
              nextOffsets.put(record.partition(), record.offset() + 1);
              if (driverHolder.isComplete()) {
                if (driverHolders.get(0) == driverHolder && ioConfig.isPauseAfterRead()) {
                  // this is the latest driver and isPauseAfterRead is set
                  // means that setEndOffset will be called, so create
                  // a new driver whose end offset will be set by that call
                  try {
                    log.info("Creating new driver as pauseAfterRead is set and the latest driver is not the last driver");
                    lockDriversList();
                    final DriverHolder nextDriverHolder = DriverHolder.getNextDriverHolder(
                        this,
                        driverHolders.get(0).getEndOffsets(),
                        maxEndOffsets,
                        ioConfig.getBaseSequenceName(),
                        fireDepartmentMetrics,
                        toolbox
                    );
                    driverHolders.add(0, nextDriverHolder);
                    nextDriverHolder.startJob(toolbox.getObjectMapper());
                    persistDriversList();
                  }
                  finally {
                    unlockDriversList();
                  }
                }
                persistAndPossiblyPublish(driverHolder);
              }
            }

            if (nextOffsets.get(record.partition()).equals(endOffsets.get(record.partition()))
                && assignment.remove(record.partition())) {
              log.info("Finished reading topic[%s], partition[%,d].", record.topic(), record.partition());
              assignPartitions(consumer, topic, assignment);
              stillReading = ioConfig.isPauseAfterRead() || !assignment.isEmpty();
            }
          }

          // check if we hit the maxRowsInSegment limit for the latest driver
          DriverHolder latestDriver;
          try {
            lockDriversList();
            latestDriver = driverHolders.size() > 0 ? driverHolders.get(0) : null;
          }
          finally {
            unlockDriversList();
          }
          if (latestDriver != null && latestDriver.isCheckPointingRequired()) {
            // time to finish this driver
            // send a call to Supervisor to check point the current highest offsets for all replicas
            // supervisor will resume the tasks with a check point which will be set as end offset of the latest driver
            pause(-1L);

            KafkaDataSourceMetadata previousCheckPoint = null;
            if (!latestDriver.getStartOffsets().equals(ioConfig.getStartPartitions().getPartitionOffsetMap())) {
              previousCheckPoint = new KafkaDataSourceMetadata(new KafkaPartitions(
                  topic,
                  latestDriver.getStartOffsets()
              ));
            }

            if (!toolbox.getTaskActionClient().submit(new CheckPointDataSourceMetadataAction(
                getDataSource(),
                ioConfig.getBaseSequenceName(),
                previousCheckPoint,
                new KafkaDataSourceMetadata(new KafkaPartitions(topic, nextOffsets))
            ))) {
              throw new ISE("Checkpoint request with offsets [%s] failed, dying", nextOffsets);
            }
          }
        }
      }
      catch (Throwable t) {
        // if any exception is thrown in the ingestion loop, task should persist pending drivers and die (skip publish)
        stopRequested.set(true);
        publishOnStop.set(false);
        throw t;
      }
      finally {
        synchronized (statusLock) {
          // If either publish on stop is set or if stop is not requested (this happens when end offsets were already set
          // to some meaningful value when the task started and the task has consumed till the end offsets)
          if (publishOnStop.get() || !stopRequested.get()) {
            status = Status.PUBLISHING;
          }
        }
        log.info("Ingestion loop finished, Persisting all pending drivers...");
        for (DriverHolder driverHolder : driverHolders) {
          if (driverHolder.driverStatus == DriverHolder.DriverStatus.OPEN || driverHolder.isComplete()) {
            driverHolder.driverStatus = DriverHolder.DriverStatus.COMPLETE;
            persistAndPossiblyPublish(driverHolder);
          } else {
            log.warn(
                "Not adding driver [%s] to persist and publish queue as it should already be there",
                driverHolder.getMetadata()
            );
          }
        }
        // add Sentinel Driver at the end so that task can wait till the persistLatch and handOffLatch is countdown
        persistAndPossiblyPublish(new DriverHolder.SentinelDriverHolder(persistLatch, handOffLatch));
      }

      persistLatch.await();
      log.info("[Shutting Down]: All drivers persisted");
      checkAndMayBeThrowException();
      handOffLatch.await();
      log.info("[Shutting Down]: All drivers handed-off");
      checkAndMayBeThrowException();
    }
    catch (Throwable t) {
      // so that when executors are interrupted by shutdownNow call, they know it is expected
      stopRequested.set(true);
      throw t;
    }
    finally {
      persistExecService.shutdownNow();
      // interrupts the publish thread so that no drivers are closed or enqueued again
      // as we will be closing all of them so that segments will be unannounced

      // all the executors should be shutdown before closing the driver to prevent deadlocks
      // persistExecutor and pushExecutor (in AppenderatorImpl) depend on each other and if there are
      // two threads trying to use them deadlock is possible
      publishExecService.shutdownNow();
      handOffExecService.shutdownNow();
      consumer.close();
      for (DriverHolder driverHolder : driverHolders) {
        driverHolder.close();
      }
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }
    }

    return success();
  }

  private void checkAndMayBeThrowException()
  {
    // check for any exception set by other executor threads than the task runner thread because of which the task should fail
    if (throwableAtomicReference.get() != null) {
      Throwables.propagate(throwableAtomicReference.get());
    }
  }

  private ListenableFuture<Object> persistAndPossiblyPublish(final DriverHolder driverHolder)
  {
    log.info("Persisting and possibly publishing driver [%s]", driverHolder.getMetadata());
    Preconditions.checkState(
        driverHolder.driverStatus == DriverHolder.DriverStatus.COMPLETE,
        String.format(
            "WTH?! Cannot persist driver which is not complete, driver status: [%s]",
            driverHolder.driverStatus
        )
    );
    driverHolder.driverStatus = DriverHolder.DriverStatus.PERSISTING;
    return persistExecService.submit(
        new Callable<Object>()
        {
          @Override
          public Object call() throws Exception
          {
            Object result = null;
            try {
              result = driverHolder.persist();
              driverHolder.driverStatus = DriverHolder.DriverStatus.PERSISTED;
              log.info("Driver [%d] persisted with result [%s]", driverHolder.getDriverIndex(), result);

              if (stopRequested.get() && !publishOnStop.get()) {
                log.warn("Skipping publish of driver [%d] as we are asked to stop", driverHolder.getDriverIndex());
              } else {
                log.info("Adding driver to publish queue, [%s]", driverHolder.getMetadata());
                publishQueue.addLast(driverHolder);
              }
            }
            catch (Exception e) {
              if (e instanceof InterruptedException && (stopRequested.get() || handOffLatch.getCount() == 0)) {
                log.warn("Interrupted while persisting driver [%d], aborting persist", driverHolder.getDriverIndex());
                return null;
              }
              log.error("Error [%s] while persisting driver [%s]", e.getMessage(), driverHolder.getMetadata());
              throwableAtomicReference.set(e);
              handOffLatch.countDown();
            }
            return result;
          }
        }
    );
  }

  private void lockDriversList() throws InterruptedException
  {
    log.debug("Thread [%s] locking drivers list", Thread.currentThread());
    driversListLock.lockInterruptibly();
  }

  private void unlockDriversList()
  {
    log.debug("Thread [%s] unlocking drivers list", Thread.currentThread());
    driversListLock.unlock();
  }

  private void restoreState(TaskToolbox toolbox) throws IOException, InterruptedException
  {
    driversRestoreFile = new File(toolbox.getTaskWorkDir(), "drivers.json");
    List<DriverHolder.DriverMetadata> persistedDrivers = ImmutableList.of();
    // check for persisted Drivers information
    if (driversRestoreFile.exists()) {
      persistedDrivers = toolbox.getObjectMapper().readValue(
          driversRestoreFile,
          new TypeReference<List<DriverHolder.DriverMetadata>>()
          {
          }
      );
    }
    if (persistedDrivers.size() > 0) {
      Collections.sort(persistedDrivers);
      log.info("Trying to restore drivers list [%s]", persistedDrivers);

      for (DriverHolder.DriverMetadata driverMetadata : persistedDrivers) {
        Preconditions.checkState(
            driverMetadata.getSequenceName().startsWith(ioConfig.getBaseSequenceName()),
            String.format(
                "Sequence Name validation failed while restoring driver with metadata [%s]",
                driverMetadata
            )
        );
        final DriverHolder driverHolder = DriverHolder.createDriverHolder(
            this,
            driverMetadata.getStartOffsets(),
            driverMetadata.getEndOffsets(),
            driverMetadata.getDriverIndex(),
            driverMetadata.getSequenceName().substring(0, driverMetadata.getSequenceName().lastIndexOf("_")),
            fireDepartmentMetrics,
            toolbox,
            driverMetadata.isLast(),
            driverMetadata.isCheckPointed(),
            driverMetadata.isMaxRowsPerSegmentLimitReached()
        );
        driverHolders.add(0, driverHolder);
        final Map<Integer, Long> restoredNextPartitionsOffset = driverHolder.startJob(toolbox.getObjectMapper());
        // Set nextOffset to be the highest offset for each partition among all persisted drivers
        for (Map.Entry<Integer, Long> partitionOffset : restoredNextPartitionsOffset.entrySet()) {
          if (!nextOffsets.containsKey(partitionOffset.getKey()) || partitionOffset.getValue() > nextOffsets.get(
              partitionOffset.getKey())) {
            nextOffsets.put(partitionOffset.getKey(), partitionOffset.getValue());
          }
        }
        nextDriverIndex = driverMetadata.getDriverIndex() + 1;

        if (driverHolder.isComplete()) {
          persistAndPossiblyPublish(driverHolder);
        }
      }
    } else {
      // create a new driver
      driverHolders.add(
          0,
          DriverHolder.getNextDriverHolder(
              this,
              ioConfig.getStartPartitions().getPartitionOffsetMap(),
              endOffsets,
              ioConfig.getBaseSequenceName(),
              fireDepartmentMetrics,
              toolbox
          )
      );
      // Start up, set up initial offsets.
      nextOffsets.putAll(driverHolders.get(0).startJob(toolbox.getObjectMapper()));
    }

    final Object checkPointsObject = getContextValue("check_points");
    List<DataSourceMetadata> checkPoints = ImmutableList.of();
    if (checkPointsObject != null) {
      checkPoints = toolbox.getObjectMapper().readValue(
          (String) checkPointsObject,
          new TypeReference<List<DataSourceMetadata>>()
          {
          }
      );
    }
    log.info("Got check points: [%s]", checkPoints);
    // Restore from check points only when there are no persisted drivers
    if (persistedDrivers.size() == 0 && checkPoints.size() > 0) {
      log.info("Trying to restore check points: [%s]", checkPoints);
      // check consistency and create and start new drivers if necessary
      Collections.reverse(checkPoints);
      int checkPointIdx = checkPoints.size() - 1;

      // There will be only driver that was created in earlier step
      Preconditions.checkState(
          driverHolders.size() == 1,
          "Found more than one driver for new task [%s]",
          driverHolders
      );
      driverHolders.get(0)
                   .setEndOffsets((((KafkaDataSourceMetadata) checkPoints.get(checkPointIdx)).getKafkaPartitions()
                                                                                             .getPartitionOffsetMap()));

      if (driverHolders.get(0).isComplete()) {
        persistAndPossiblyPublish(driverHolders.get(0));
      }
      checkPointIdx--;

      // create more drivers corresponding to the check points that this task does not know about
      while (checkPointIdx >= 0) {
        driverHolders.add(
            0,
            DriverHolder.getNextDriverHolder(
                this,
                (((KafkaDataSourceMetadata) checkPoints.get(checkPointIdx + 1)).getKafkaPartitions()
                                                                               .getPartitionOffsetMap()),
                (((KafkaDataSourceMetadata) checkPoints.get(checkPointIdx)).getKafkaPartitions()
                                                                           .getPartitionOffsetMap()),
                ioConfig.getBaseSequenceName(),
                fireDepartmentMetrics,
                toolbox,
                true
            )
        );
        nextOffsets.putAll(driverHolders.get(0).startJob(toolbox.getObjectMapper()));
        if (driverHolders.get(0).isComplete()) {
          persistAndPossiblyPublish(driverHolders.get(0));
        }
        checkPointIdx--;
      }
    }
    // save latest state on disk
    persistDriversList();

    if (driverHolders.get(0).isLast()) {
      // recovered a driver which happens to be the last driver for this task
      // set the end offsets for this task so that the task shutdowns eventually
      endOffsets.putAll(driverHolders.get(0).getEndOffsets());
    }
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @POST
  @Path("/stop")
  @Override
  public void stopGracefully()
  {
    log.info("Stopping gracefully (status: [%s])", status);
    stopRequested.set(true);
    // don't wait for publishes/handoff to complete
    handOffLatch.countDown();

    synchronized (statusLock) {
      if (status == Status.PUBLISHING) {
        // no need to try to resume, return immediately
        return;
      }
    }

    try {
      if (pauseLock.tryLock(LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          if (pauseRequested) {
            pauseRequested = false;
            shouldResume.signalAll();
          }
        }
        finally {
          pauseLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pauseLock before timeout, interrupting run thread");
        runThread.interrupt();
        return;
      }

      if (pollRetryLock.tryLock(LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          isAwaitingRetry.signalAll();
        }
        finally {
          pollRetryLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pollRetryLock before timeout, interrupting run thread");
        runThread.interrupt();
      }
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (driverHolders == null || driverHolders.size() == 0 || toolbox == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }
    final QueryRunnerFactory<T, Query<T>> queryRunnerFactory = toolbox.getQueryRunnerFactoryConglomerate()
                                                                      .findFactory(query);
    return queryRunnerFactory.getToolchest().mergeResults(
        queryRunnerFactory.mergeRunners(
            toolbox.getQueryExecutorService(),
            Iterables.transform(
                driverHolders,
                new Function<DriverHolder, QueryRunner<T>>()
                {
                  @Override
                  public QueryRunner<T> apply(final DriverHolder input)
                  {
                    return new QueryRunner<T>()
                    {
                      @Override
                      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
                      {
                        return query.run(input.getDriver().getAppenderator(), responseContext);
                      }
                    };
                  }
                }
            )
        )
    );
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Status getStatus()
  {
    return status;
  }

  @GET
  @Path("/offsets/current")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Long> getCurrentOffsets()
  {
    return nextOffsets;
  }

  @GET
  @Path("/offsets/end")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Long> getEndOffsets()
  {
    return endOffsets;
  }

  @POST
  @Path("/offsets/end")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setEndOffsets(
      Map<Integer, Long> offsets,
      @QueryParam("resume") @DefaultValue("false") final boolean resume,
      @QueryParam("finish") @DefaultValue("true") final boolean finish
      // this field is only for internal purposes, should never be set by users
  ) throws InterruptedException
  {
    if (offsets == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity("Request body must contain a map of { partition:endOffset }")
                     .build();
    } else if (!endOffsets.keySet().containsAll(offsets.keySet())) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         String.format(
                             "Request contains partitions not being handled by this task, my partitions: %s",
                             endOffsets.keySet()
                         )
                     )
                     .build();
    }

    if (status == Status.NOT_STARTED || status == Status.STARTING) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("Task has not started running yet!").build();
    }

    pauseLock.lockInterruptibly();
    try {
      if (!isPaused()) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity("Task must be paused before changing the end offsets")
                       .build();
      }

      for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
        if (entry.getValue().compareTo(nextOffsets.get(entry.getKey())) < 0) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity(
                             String.format(
                                 "End offset must be >= current offset for partition [%s] (current: %s)",
                                 entry.getKey(),
                                 nextOffsets.get(entry.getKey())
                             )
                         )
                         .build();
        }
      }

      lockDriversList();
      Preconditions.checkState(driverHolders.size() > 0, "WTH?! No drivers found to set end offsets");
      final DriverHolder driverHolder = driverHolders.get(0);

      try {
        if (driverHolder.isCheckPointed()) {
          // this should only happen when we got another setEndOffsets call after the final setEndOffsets call
          // check for consistency and duplicate request
          Preconditions.checkState(
              endOffsets.equals(driverHolder.getEndOffsets()),
              "WTH?! End offsets for task [%s] and latest driver [%s] do not match",
              endOffsets,
              driverHolder.getMetadata()
          );
          // ignore duplicate requests
          if (offsets.equals(driverHolder.getEndOffsets())) {
            log.warn(
                "end offsets already set to [%s], ignoring duplicate request to set to [%s]",
                driverHolder.getEndOffsets(),
                offsets
            );
            return Response.ok(endOffsets).build();
          } else {
            throw new ISE(
                "WTH?! end offsets set to [%s], ignoring request to set to [%s]",
                driverHolder.getEndOffsets(),
                offsets
            );
          }
        }
        driverHolder.setEndOffsets(offsets);

        if (finish) {
          // set the last flag, useful while restoring state from disk to set endOffsets
          driverHolder.setLast();
          endOffsets.putAll(offsets);
        } else {
          // create next driver
          final DriverHolder nextDriverHolder = DriverHolder.getNextDriverHolder(
              this,
              driverHolder.getEndOffsets(), //previous driver endOffsets
              endOffsets, // task endOffsets
              ioConfig.getBaseSequenceName(),
              fireDepartmentMetrics,
              toolbox
          );
          driverHolders.add(0, nextDriverHolder);
          nextDriverHolder.startJob(toolbox.getObjectMapper());
        }
        persistDriversList();
        if (driverHolder.isComplete()) {
          persistAndPossiblyPublish(driverHolder);
        }
      }
      catch (Exception e) {
        log.error(e, "Exception while setting end offsets [%s]", offsets);
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
      }
      finally {
        unlockDriversList();
      }
    }
    finally {
      pauseLock.unlock();
    }

    if (resume) {
      resume();
    }

    return Response.ok(endOffsets).build();
  }

  private void persistDriversList() throws IOException, InterruptedException
  {
    try {
      lockDriversList();
      log.info("Persisting drivers list [%s]", driverHolders);
      toolbox.getObjectMapper().writerWithType(new TypeReference<List<DriverHolder.DriverMetadata>>()
      {
      }).writeValue(driversRestoreFile, Lists.newArrayList(Iterables.transform(
          driverHolders, new Function<DriverHolder, DriverHolder.DriverMetadata>()
          {
            @Override
            public DriverHolder.DriverMetadata apply(DriverHolder input)
            {
              return input.getMetadata();
            }
          }
      )));
    }
    finally {
      unlockDriversList();
    }
  }

  /**
   * Signals the ingestion loop to pause.
   *
   * @param timeout how long to pause for before resuming in milliseconds, <= 0 means indefinitely
   *
   * @return one of the following Responses: 400 Bad Request if the task has started publishing; 202 Accepted if the
   * method has timed out and returned before the task has paused; 200 OK with a map of the current partition offsets
   * in the response body if the task successfully paused
   */
  @POST
  @Path("/pause")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pause(@QueryParam("timeout") @DefaultValue("0") final long timeout)
      throws InterruptedException
  {
    if (!(status == Status.PAUSED || status == Status.READING)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(String.format("Can't pause, task is not in a pausable state (state: [%s])", status))
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      pauseMillis = timeout <= 0 ? PAUSE_FOREVER : timeout;
      pauseRequested = true;

      pollRetryLock.lockInterruptibly();
      try {
        isAwaitingRetry.signalAll();
      }
      finally {
        pollRetryLock.unlock();
      }

      if (isPaused()) {
        shouldResume.signalAll(); // kick the monitor so it re-awaits with the new pauseMillis
      }

      long nanos = TimeUnit.SECONDS.toNanos(2);
      while (!isPaused()) {
        if (nanos <= 0L) {
          return Response.status(Response.Status.ACCEPTED)
                         .entity("Request accepted but task has not yet paused")
                         .build();
        }
        nanos = hasPaused.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }

    try {
      return Response.ok().entity(toolbox.getObjectMapper().writeValueAsString(getCurrentOffsets())).build();
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @POST
  @Path("/resume")
  public void resume() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      pauseRequested = false;
      shouldResume.signalAll();

      long nanos = TimeUnit.SECONDS.toNanos(5);
      while (isPaused()) {
        if (nanos <= 0L) {
          throw new RuntimeException("Resume command was not accepted within 5 seconds");
        }
        nanos = shouldResume.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }
  }

  @GET
  @Path("/time/start")
  @Produces(MediaType.APPLICATION_JSON)
  public DateTime getStartTime()
  {
    return startTime;
  }

  @VisibleForTesting
  FireDepartmentMetrics getFireDepartmentMetrics()
  {
    return fireDepartmentMetrics;
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox, File basePersistDirectory)
  {
    return Appenderators.createRealtime(
        dataSchema,
        tuningConfig.withBasePersistDirectory(basePersistDirectory),
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

  FiniteAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final FireDepartmentMetrics metrics
  )
  {
    return new FiniteAppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        tuningConfig.getMaxRowsPerSegment(),
        tuningConfig.getHandoffConditionTimeout(),
        metrics
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

  private Set<Integer> assignPartitionsAndSeekToNext(KafkaConsumer consumer, String topic)
  {
    // Initialize consumer assignment.
    final Set<Integer> assignment = Sets.newHashSet();
    for (Map.Entry<Integer, Long> entry : nextOffsets.entrySet()) {
      final long endOffset = endOffsets.get(entry.getKey());
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

    return assignment;
  }

  /**
   * Checks if the pauseRequested flag was set and if so blocks:
   * a) if pauseMillis == PAUSE_FOREVER, until pauseRequested is cleared
   * b) if pauseMillis != PAUSE_FOREVER, until pauseMillis elapses -or- pauseRequested is cleared
   * <p/>
   * If pauseMillis is changed while paused, the new pause timeout will be applied. This allows adjustment of the
   * pause timeout (making a timed pause into an indefinite pause and vice versa is valid) without having to resume
   * and ensures that the loop continues to stay paused without ingesting any new events. You will need to signal
   * shouldResume after adjusting pauseMillis for the new value to take effect.
   * <p/>
   * Sets paused = true and signals paused so callers can be notified when the pause command has been accepted.
   * <p/>
   * Additionally, pauses if all partitions assignments have been read and pauseAfterRead flag is set.
   *
   * @return true if a pause request was handled, false otherwise
   */
  private boolean possiblyPause(Set<Integer> assignment) throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      if (ioConfig.isPauseAfterRead() && assignment.isEmpty()) {
        pauseMillis = PAUSE_FOREVER;
        pauseRequested = true;
      }

      if (pauseRequested) {
        status = Status.PAUSED;
        long nanos = 0;
        hasPaused.signalAll();

        while (pauseRequested) {
          if (pauseMillis == PAUSE_FOREVER) {
            log.info("Pausing ingestion until resumed");
            shouldResume.await();
          } else {
            if (pauseMillis > 0) {
              log.info("Pausing ingestion for [%,d] ms", pauseMillis);
              nanos = TimeUnit.MILLISECONDS.toNanos(pauseMillis);
              pauseMillis = 0;
            }
            if (nanos <= 0L) {
              pauseRequested = false; // timeout elapsed
            }
            nanos = shouldResume.awaitNanos(nanos);
          }
        }

        status = Status.READING;
        shouldResume.signalAll();
        log.info("Ingestion loop resumed");
        return true;
      }
    }
    finally {
      pauseLock.unlock();
    }

    return false;
  }

  private void possiblyResetOffsetsOrWait(
      Map<TopicPartition, Long> outOfRangePartitions,
      KafkaConsumer<byte[], byte[]> consumer,
      TaskToolbox taskToolbox
  ) throws InterruptedException, IOException
  {
    final Map<TopicPartition, Long> resetPartitions = Maps.newHashMap();
    boolean doReset = false;
    if (tuningConfig.isResetOffsetAutomatically()) {
      for (Map.Entry<TopicPartition, Long> outOfRangePartition : outOfRangePartitions.entrySet()) {
        final TopicPartition topicPartition = outOfRangePartition.getKey();
        final long nextOffset = outOfRangePartition.getValue();
        // seek to the beginning to get the least available offset
        consumer.seekToBeginning(Collections.singletonList(topicPartition));
        final long leastAvailableOffset = consumer.position(topicPartition);
        // reset the seek
        consumer.seek(topicPartition, nextOffset);
        // Reset consumer offset if resetOffsetAutomatically is set to true
        // and the current message offset in the kafka partition is more than the
        // next message offset that we are trying to fetch
        if (leastAvailableOffset > nextOffset) {
          doReset = true;
          resetPartitions.put(topicPartition, nextOffset);
        }
      }
    }

    if (doReset) {
      sendResetRequestAndWait(resetPartitions, taskToolbox);
    } else {
      log.warn("Retrying in %dms", POLL_RETRY_MS);
      pollRetryLock.lockInterruptibly();
      try {
        long nanos = TimeUnit.MILLISECONDS.toNanos(POLL_RETRY_MS);
        while (nanos > 0L && !pauseRequested && !stopRequested.get()) {
          nanos = isAwaitingRetry.awaitNanos(nanos);
        }
      }
      finally {
        pollRetryLock.unlock();
      }
    }
  }

  private void sendResetRequestAndWait(Map<TopicPartition, Long> outOfRangePartitions, TaskToolbox taskToolbox)
      throws IOException
  {
    Map<Integer, Long> partitionOffsetMap = Maps.newHashMap();
    for (Map.Entry<TopicPartition, Long> outOfRangePartition : outOfRangePartitions.entrySet()) {
      partitionOffsetMap.put(outOfRangePartition.getKey().partition(), outOfRangePartition.getValue());
    }
    boolean result = taskToolbox.getTaskActionClient()
                                .submit(new ResetDataSourceMetadataAction(
                                    getDataSource(),
                                    new KafkaDataSourceMetadata(new KafkaPartitions(
                                        ioConfig.getStartPartitions()
                                                .getTopic(),
                                        partitionOffsetMap
                                    ))
                                ));

    if (result) {
      log.makeAlert("Resetting Kafka offsets for datasource [%s]", getDataSource())
         .addData("partitions", partitionOffsetMap.keySet())
         .emit();
      // wait for being killed by supervisor
      try {
        pause(-1);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Got interrupted while pausing task");
      }
    } else {
      log.makeAlert("Failed to send reset request for partitions [%s]", partitionOffsetMap.keySet()).emit();
    }
  }
}
