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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.common.guava.ThreadRenamingCallable;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.segment.realtime.plumber.Plumber;
import org.apache.druid.segment.realtime.plumber.RejectionPolicy;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import org.apache.druid.segment.realtime.plumber.VersioningPolicy;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AppenderatorPlumber implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(AppenderatorPlumber.class);
  private static final int WARN_DELAY = 1000;

  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final RejectionPolicy rejectionPolicy;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final SegmentPublisher segmentPublisher;
  private final SegmentHandoffNotifier handoffNotifier;
  private final Object handoffCondition = new Object();
  private final ConcurrentMap<Long, SegmentIdWithShardSpec> segments = new ConcurrentHashMap<>();
  private final Appenderator appenderator;

  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;
  private volatile boolean cleanShutdown = true;
  private volatile ScheduledExecutorService scheduledExecutor = null;

  private volatile Supplier<Committer> lastCommitterSupplier = null;

  public AppenderatorPlumber(
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentAnnouncer segmentAnnouncer,
      SegmentPublisher segmentPublisher,
      SegmentHandoffNotifier handoffNotifier,
      Appenderator appenderator
  )
  {
    this.schema = schema;
    this.config = config;
    this.rejectionPolicy = config.getRejectionPolicyFactory().create(config.getWindowPeriod());
    this.metrics = metrics;
    this.segmentAnnouncer = segmentAnnouncer;
    this.segmentPublisher = segmentPublisher;
    this.handoffNotifier = handoffNotifier;
    this.appenderator = appenderator;

    log.info("Creating plumber using rejectionPolicy[%s]", getRejectionPolicy());
  }

  public Map<Long, SegmentIdWithShardSpec> getSegmentsView()
  {
    return ImmutableMap.copyOf(segments);
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public RealtimeTuningConfig getConfig()
  {
    return config;
  }

  public RejectionPolicy getRejectionPolicy()
  {
    return rejectionPolicy;
  }

  @Override
  public Object startJob()
  {
    handoffNotifier.start();
    Object retVal = appenderator.startJob();
    initializeExecutors();
    startPersistThread();
    // Push pending sinks bootstrapped from previous run
    mergeAndPush();
    return retVal;
  }

  @Override
  public IncrementalIndexAddResult add(InputRow row, Supplier<Committer> committerSupplier)
      throws IndexSizeExceededException
  {
    final SegmentIdWithShardSpec identifier = getSegmentIdentifier(row.getTimestampFromEpoch());
    if (identifier == null) {
      return Plumber.THROWAWAY;
    }

    try {
      final Appenderator.AppenderatorAddResult addResult = appenderator.add(identifier, row, committerSupplier);
      lastCommitterSupplier = committerSupplier;
      return new IncrementalIndexAddResult(addResult.getNumRowsInSegment(), 0, addResult.getParseException());
    }
    catch (SegmentNotWritableException e) {
      // Segment already started handoff
      return Plumber.NOT_WRITABLE;
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
      {
        return queryPlus.run(appenderator, responseContext);
      }
    };
  }

  @Override
  public void persist(final Committer committer)
  {
    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    appenderator.persistAll(committer);

    final long startDelay = runExecStopwatch.elapsed(TimeUnit.MILLISECONDS);
    metrics.incrementPersistBackPressureMillis(startDelay);
    if (startDelay > WARN_DELAY) {
      log.warn("Ingestion was throttled for [%,d] millis because persists were pending.", startDelay);
    }
    runExecStopwatch.stop();
  }

  @Override
  public void finishJob()
  {
    log.info("Shutting down...");

    shuttingDown = true;

    List<SegmentIdWithShardSpec> pending = appenderator.getSegments();
    if (pending.isEmpty()) {
      log.info("No segments to hand off.");
    } else {
      log.info("Pushing segments: %s", Joiner.on(", ").join(pending));
    }

    try {
      if (lastCommitterSupplier != null) {
        // Push all remaining data
        mergeAndPush();
      }

      synchronized (handoffCondition) {
        while (!segments.isEmpty()) {
          log.info("Waiting to hand off: %s", Joiner.on(", ").join(pending));
          handoffCondition.wait();
          pending = appenderator.getSegments();
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      stopped = true;
      handoffNotifier.close();
      shutdownExecutors();
      appenderator.close();
    }

    if (!cleanShutdown) {
      throw new ISE("Exception occurred during persist and merge.");
    }
  }

  private SegmentIdWithShardSpec getSegmentIdentifier(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    DateTime truncatedDateTime = segmentGranularity.bucketStart(DateTimes.utc(timestamp));
    final long truncatedTime = truncatedDateTime.getMillis();

    SegmentIdWithShardSpec retVal = segments.get(truncatedTime);

    if (retVal == null) {
      final Interval interval = new Interval(
          truncatedDateTime,
          segmentGranularity.increment(truncatedDateTime)
      );

      retVal = new SegmentIdWithShardSpec(
          schema.getDataSource(),
          interval,
          versioningPolicy.getVersion(interval),
          config.getShardSpec()
      );
      addSegment(retVal);

    }

    return retVal;
  }

  protected void initializeExecutors()
  {
    if (scheduledExecutor == null) {
      scheduledExecutor = Execs.scheduledSingleThreaded("plumber_scheduled_%d");
    }
  }

  protected void shutdownExecutors()
  {
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  private void addSegment(final SegmentIdWithShardSpec identifier)
  {
    segments.put(identifier.getInterval().getStartMillis(), identifier);
    try {
      segmentAnnouncer.announceSegment(
          new DataSegment(
              identifier.getDataSource(),
              identifier.getInterval(),
              identifier.getVersion(),
              ImmutableMap.of(),
              ImmutableList.of(),
              ImmutableList.of(),
              identifier.getShardSpec(),
              null,
              0
          )
      );
    }
    catch (IOException e) {
      log.makeAlert(e, "Failed to announce new segment[%s]", identifier.getDataSource())
         .addData("interval", identifier.getInterval())
         .emit();
    }
  }

  public void dropSegment(final SegmentIdWithShardSpec identifier)
  {
    log.info("Dropping segment: %s", identifier);
    segments.remove(identifier.getInterval().getStartMillis());

    Futures.addCallback(
        appenderator.drop(identifier),
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object result)
          {
            log.info("Dropped segment: %s", identifier);
          }

          @Override
          public void onFailure(Throwable e)
          {
            // TODO: Retry?
            log.warn(e, "Failed to drop segment: %s", identifier);
          }
        }
    );
  }

  private void startPersistThread()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final DateTime truncatedNow = segmentGranularity.bucketStart(DateTimes.nowUtc());
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        DateTimes.nowUtc().plus(
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    String threadName = StringUtils.format(
        "%s-overseer-%d",
        schema.getDataSource(),
        config.getShardSpec().getPartitionNum()
    );
    ThreadRenamingCallable<ScheduledExecutors.Signal> threadRenamingCallable =
        new ThreadRenamingCallable<ScheduledExecutors.Signal>(threadName)
        {
          @Override
          public ScheduledExecutors.Signal doCall()
          {
            if (stopped) {
              log.info("Stopping merge-n-push overseer thread");
              return ScheduledExecutors.Signal.STOP;
            }

            mergeAndPush();

            if (stopped) {
              log.info("Stopping merge-n-push overseer thread");
              return ScheduledExecutors.Signal.STOP;
            } else {
              return ScheduledExecutors.Signal.REPEAT;
            }
          }
        };
    Duration initialDelay = new Duration(
        System.currentTimeMillis(),
        segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
    );
    Duration rate = new Duration(truncatedNow, segmentGranularity.increment(truncatedNow));
    ScheduledExecutors.scheduleAtFixedRate(scheduledExecutor, initialDelay, rate, threadRenamingCallable);
  }

  private void mergeAndPush()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    log.info("Starting merge and push.");
    DateTime minTimestampAsDate = segmentGranularity.bucketStart(
        DateTimes.utc(Math.max(windowMillis, rejectionPolicy.getCurrMaxTime().getMillis()) - windowMillis)
    );
    long minTimestamp = minTimestampAsDate.getMillis();

    final List<SegmentIdWithShardSpec> appenderatorSegments = appenderator.getSegments();
    final List<SegmentIdWithShardSpec> segmentsToPush = new ArrayList<>();

    if (shuttingDown) {
      log.info("Found [%,d] segments. Attempting to hand off all of them.", appenderatorSegments.size());
      segmentsToPush.addAll(appenderatorSegments);
    } else {
      log.info(
          "Found [%,d] segments. Attempting to hand off segments that start before [%s].",
          appenderatorSegments.size(),
          minTimestampAsDate
      );

      for (SegmentIdWithShardSpec segment : appenderatorSegments) {
        final Long intervalStart = segment.getInterval().getStartMillis();
        if (intervalStart < minTimestamp) {
          log.info("Adding entry [%s] for merge and push.", segment);
          segmentsToPush.add(segment);
        } else {
          log.info(
              "Skipping persist and merge for entry [%s] : Start time [%s] >= [%s] min timestamp required in this run. Segment will be picked up in a future run.",
              segment,
              DateTimes.utc(intervalStart),
              minTimestampAsDate
          );
        }
      }
    }

    log.info("Found [%,d] segments to persist and merge", segmentsToPush.size());

    final Function<Throwable, Void> errorHandler = new Function<Throwable, Void>()
    {
      @Override
      public Void apply(Throwable throwable)
      {
        final List<String> segmentIdentifierStrings = Lists.transform(
            segmentsToPush,
            SegmentIdWithShardSpec::toString
        );

        log.makeAlert(throwable, "Failed to publish merged indexes[%s]", schema.getDataSource())
           .addData("segments", segmentIdentifierStrings)
           .emit();

        if (shuttingDown) {
          // We're trying to shut down, and these segments failed to push. Let's just get rid of them.
          // This call will also delete possibly-partially-written files, so we don't need to do it explicitly.
          cleanShutdown = false;
          for (SegmentIdWithShardSpec identifier : segmentsToPush) {
            dropSegment(identifier);
          }
        }

        return null;
      }
    };

    // WARNING: Committers.nil() here means that on-disk data can get out of sync with committing.
    Futures.addCallback(
        appenderator.push(segmentsToPush, Committers.nil(), false),
        new FutureCallback<SegmentsAndCommitMetadata>()
        {
          @Override
          public void onSuccess(SegmentsAndCommitMetadata result)
          {
            // Immediately publish after pushing
            for (DataSegment pushedSegment : result.getSegments()) {
              try {
                segmentPublisher.publishSegment(pushedSegment);
              }
              catch (Exception e) {
                errorHandler.apply(e);
              }
            }

            log.info("Published [%,d] sinks.", segmentsToPush.size());
          }

          @Override
          public void onFailure(Throwable e)
          {
            log.warn(e, "Failed to push [%,d] segments.", segmentsToPush.size());
            errorHandler.apply(e);
          }
        }
    );
  }
}
