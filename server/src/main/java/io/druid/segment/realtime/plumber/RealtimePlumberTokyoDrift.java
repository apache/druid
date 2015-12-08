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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.FilteredServerView;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.ServerViewWatcher;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class RealtimePlumberTokyoDrift implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumberTokyoDrift.class);

  private final Appenderator appenderator;
  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final RejectionPolicy rejectionPolicy;
  private final FilteredServerView serverView;
  private final SegmentPublisher segmentPublisher;

  private final Object handoffMonitor = new Object();
  private volatile ExecutorService serverViewExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;
  private volatile ServerViewWatcher serverViewWatcher = null;
  private volatile Supplier<Committer> lastCommitterSupplier = null;
  private volatile boolean cleanShutdown = true;
  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;

  public RealtimePlumberTokyoDrift(
      final Appenderator appenderator,
      final DataSchema schema,
      final RealtimeTuningConfig config,
      final FilteredServerView serverView,
      final SegmentPublisher segmentPublisher
  )
  {
    this.appenderator = appenderator;
    this.schema = schema;
    this.config = config.withBasePersistDirectory(
        makeBasePersistSubdirectory(
            config.getBasePersistDirectory(),
            schema.getDataSource(),
            config.getShardSpec()
        )
    );
    this.rejectionPolicy = config.getRejectionPolicyFactory().create(config.getWindowPeriod());
    this.serverView = serverView;
    this.segmentPublisher = segmentPublisher;
  }

  private static File makeBasePersistSubdirectory(
      final File basePersistDirectory,
      final String dataSource,
      final ShardSpec shardSpec
  )
  {
    final File dataSourceDirectory = new File(basePersistDirectory, dataSource);
    return new File(dataSourceDirectory, String.valueOf(shardSpec.getPartitionNum()));
  }

  @Override
  public Object startJob()
  {
    final Object retVal = appenderator.startJob();
    initializeExecutors();
    startPersistThread();
    initializeServerViewWatcher();
    mergeAndPush();
    return retVal;
  }

  @Override
  public int add(
      final InputRow row,
      final Supplier<Committer> committerSupplier
  ) throws IndexSizeExceededException
  {
    if (!rejectionPolicy.accept(row.getTimestampFromEpoch())) {
      return -1;
    }

    final Interval segmentInterval = schema.getGranularitySpec()
                                           .getSegmentGranularity()
                                           .bucket(row.getTimestamp());

    final SegmentIdentifier segmentIdentifier = new SegmentIdentifier(
        appenderator.getDataSource(),
        segmentInterval,
        config.getVersioningPolicy().getVersion(segmentInterval),
        config.getShardSpec()
    );

    try {
      final int numRows = appenderator.add(segmentIdentifier, row, committerSupplier);
      lastCommitterSupplier = committerSupplier;
      return numRows;
    }
    catch (SegmentNotWritableException e) {
      // Segment already started handoff
      return -1;
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
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
  public void persist(Committer committer)
  {
    appenderator.persistAll(committer);
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

      synchronized (handoffMonitor) {
        while (!pending.isEmpty()) {
          log.info("Waiting to hand off: %s", Joiner.on(", ").join(pending));
          handoffMonitor.wait();
          pending = appenderator.getSegments();
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      stopped = true;
      shutdownServerViewWatcher();
      shutdownExecutors();
      appenderator.close();
    }

    if (!cleanShutdown) {
      throw new ISE("Exception occurred during persist and merge.");
    }
  }

  private void initializeExecutors()
  {
    if (scheduledExecutor == null) {
      scheduledExecutor = Execs.scheduledSingleThreaded("plumber_scheduled_%d");
    }

    if (serverViewExecutor == null) {
      serverViewExecutor = Execs.singleThreaded("plumber_serverview_%d");
    }
  }

  private void shutdownExecutors()
  {
    serverViewExecutor.shutdownNow();
    scheduledExecutor.shutdownNow();
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

    final List<SegmentIdentifier> segments = appenderator.getSegments();
    final List<SegmentIdentifier> segmentsToPush = Lists.newArrayList();

    if (shuttingDown) {
      log.info(
          "Found [%,d] segments. Attempting to hand off all of them.",
          segments.size()
      );

      segmentsToPush.addAll(segments);
    } else {
      log.info(
          "Found [%,d] segments. Attempting to hand off segments that start before [%s].",
          segments.size(),
          minTimestampAsDate
      );

      for (SegmentIdentifier segment : segments) {
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

    log.info("Found [%,d] sinks to persist and merge", segmentsToPush.size());

    // WARNING: Committers.nil() here means that on-disk data can get out of sync with committing
    ListenableFuture<?> publishFuture = Futures.transform(
        appenderator.push(segmentsToPush, Committers.nil()),
        new Function<SegmentsAndMetadata, Object>()
        {
          @Override
          public Object apply(SegmentsAndMetadata pushedSegmentsAndMetadata)
          {
            // Immediately publish after pushing
            for (DataSegment pushedSegment : pushedSegmentsAndMetadata.getSegments()) {
              try {
                segmentPublisher.publishSegment(pushedSegment);
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }

            return null;
          }
        }
    );

    Futures.addCallback(
        publishFuture,
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object result)
          {
            log.info("Published [%,d] sinks.", segmentsToPush.size());
          }

          @Override
          public void onFailure(Throwable e)
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

            log.makeAlert(e, "Failed to publish merged indexes[%s]", schema.getDataSource())
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
          }
        }
    );
  }

  private void initializeServerViewWatcher()
  {
    if (serverViewWatcher == null) {
      serverViewWatcher = new ServerViewWatcher(
          appenderator,
          serverView,
          serverViewExecutor,
          new ServerViewWatcher.Callback()
          {
            @Override
            public void notify(SegmentIdentifier pending, DruidServerMetadata server, DataSegment servedSegment)
            {
              dropSegment(pending);
            }
          }
      );

      serverViewWatcher.start();
    }
  }

  private void shutdownServerViewWatcher()
  {
    serverViewWatcher.close();
  }

  private void dropSegment(final SegmentIdentifier identifier)
  {
    log.info("Dropping segment: %s", identifier);

    Futures.addCallback(
        appenderator.drop(identifier),
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object result)
          {
            synchronized (handoffMonitor) {
              handoffMonitor.notifyAll();
            }
          }

          @Override
          public void onFailure(Throwable e)
          {
            log.warn(e, "Failed to drop segment: %s", identifier);
            synchronized (handoffMonitor) {
              handoffMonitor.notifyAll();
            }
          }
        }
    );
  }
}
