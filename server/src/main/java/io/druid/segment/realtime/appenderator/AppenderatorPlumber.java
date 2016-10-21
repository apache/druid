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

package io.druid.segment.realtime.appenderator;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.metamx.emitter.EmittingLogger;

import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.RejectionPolicy;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
  private final Map<Long, SegmentIdentifier> segments = Maps.newConcurrentMap();
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

  public Map<Long, SegmentIdentifier> getSegmentsView() {
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
  public int add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
  {
    final SegmentIdentifier identifier = getSegmentIdentifier(row.getTimestampFromEpoch());
    if (identifier == null) {
      return -1;
    }

    final int numRows;

    try {
      numRows = appenderator.add(identifier, row, committerSupplier);
      lastCommitterSupplier = committerSupplier;
      return numRows;
    }
    catch (SegmentNotWritableException e) {
      // Segment already started handoff
      return -1;
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
      {
        return query.run(appenderator, responseContext);
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

    List<SegmentIdentifier> pending = appenderator.getSegments();
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
      throw Throwables.propagate(e);
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

  private SegmentIdentifier getSegmentIdentifier(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    final long truncatedTime = segmentGranularity.truncate(new DateTime(timestamp)).getMillis();

    SegmentIdentifier retVal = segments.get(truncatedTime);

    if (retVal == null) {
      final Interval interval = new Interval(
          new DateTime(truncatedTime),
          segmentGranularity.increment(new DateTime(truncatedTime))
      );

      retVal = new SegmentIdentifier(
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

  private void addSegment(final SegmentIdentifier identifier)
  {
    segments.put(identifier.getInterval().getStartMillis(), identifier);
    try {
      segmentAnnouncer.announceSegment(
          new DataSegment(
              identifier.getDataSource(),
              identifier.getInterval(),
              identifier.getVersion(),
              ImmutableMap.<String, Object>of(),
              ImmutableList.<String>of(),
              ImmutableList.<String>of(),
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

  public void dropSegment(final SegmentIdentifier identifier)
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

    final DateTime truncatedNow = segmentGranularity.truncate(new DateTime());
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            scheduledExecutor,
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            ),
            new Duration(truncatedNow, segmentGranularity.increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-overseer-%d",
                    schema.getDataSource(),
                    config.getShardSpec().getPartitionNum()
                )
            )
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
            }
        );
  }

  private void mergeAndPush()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    log.info("Starting merge and push.");
    DateTime minTimestampAsDate = segmentGranularity.truncate(
        new DateTime(
            Math.max(
                windowMillis,
                rejectionPolicy.getCurrMaxTime()
                               .getMillis()
            )
            - windowMillis
        )
    );
    long minTimestamp = minTimestampAsDate.getMillis();

    final List<SegmentIdentifier> appenderatorSegments = appenderator.getSegments();
    final List<SegmentIdentifier> segmentsToPush = Lists.newArrayList();

    if (shuttingDown) {
      log.info("Found [%,d] segments. Attempting to hand off all of them.", appenderatorSegments.size());
      segmentsToPush.addAll(appenderatorSegments);
    } else {
      log.info(
          "Found [%,d] segments. Attempting to hand off segments that start before [%s].",
          appenderatorSegments.size(),
          minTimestampAsDate
      );

      for (SegmentIdentifier segment : appenderatorSegments) {
        final Long intervalStart = segment.getInterval().getStartMillis();
        if (intervalStart < minTimestamp) {
          log.info("Adding entry [%s] for merge and push.", segment);
          segmentsToPush.add(segment);
        } else {
          log.info(
              "Skipping persist and merge for entry [%s] : Start time [%s] >= [%s] min timestamp required in this run. Segment will be picked up in a future run.",
              segment,
              new DateTime(intervalStart),
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
            new Function<SegmentIdentifier, String>()
            {
              @Override
              public String apply(SegmentIdentifier input)
              {
                return input.getIdentifierAsString();
              }
            }
        );

        log.makeAlert(throwable, "Failed to publish merged indexes[%s]", schema.getDataSource())
           .addData("segments", segmentIdentifierStrings)
           .emit();

        if (shuttingDown) {
          // We're trying to shut down, and these segments failed to push. Let's just get rid of them.
          // This call will also delete possibly-partially-written files, so we don't need to do it explicitly.
          cleanShutdown = false;
          for (SegmentIdentifier identifier : segmentsToPush) {
            dropSegment(identifier);
          }
        }

        return null;
      }
    };

    // WARNING: Committers.nil() here means that on-disk data can get out of sync with committing.
    Futures.addCallback(
        appenderator.push(segmentsToPush, Committers.nil()),
        new FutureCallback<SegmentsAndMetadata>()
        {
          @Override
          public void onSuccess(SegmentsAndMetadata result)
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
