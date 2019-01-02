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

package org.apache.druid.extensions.watermarking;


import com.google.common.base.Predicates;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ServerView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollector;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollectorServerView;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.watermarks.WatermarkCursorFactory;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

@ManageLifecycle
public class WatermarkCollector
{
  public static final String SERVICE_NAME = "watermark-collector";
  private static final Logger log = new Logger(WatermarkCollector.class);
  private final DruidNode self;
  private final WatermarkCollectorConfig config;
  private final ServiceAnnouncer serviceAnnouncer;
  private final ServiceEmitter serviceEmitter;
  private final TimelineMetadataCollectorServerView serverView;
  private final FilteredServerInventoryView serverInventoryView;
  private final WatermarkSource watermarkSource;
  private final WatermarkSink watermarkSink;
  private final Striped<Lock> datasourceLock;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final LifecycleLock initializedTimelinesLifecycleLock = new LifecycleLock();
  private final TimelineMetadataCollector<DateTime> collector;
  private ExecutorService executor;

  @Inject
  public WatermarkCollector(
      WatermarkCollectorConfig config,
      ServiceAnnouncer serviceAnnouncer,
      ServiceEmitter serviceEmitter,
      TimelineMetadataCollectorServerView view,
      FilteredServerInventoryView serverInventoryView,
      WatermarkSource source,
      WatermarkSink sink,
      List<WatermarkCursorFactory> cursorFactories,
      @Self DruidNode node
  )
  {
    this.self = node;
    this.config = config;
    this.serviceAnnouncer = serviceAnnouncer;
    this.serviceEmitter = serviceEmitter;
    this.serverView = view;
    this.serverInventoryView = serverInventoryView;
    this.watermarkSource = source;
    this.watermarkSink = sink;
    this.datasourceLock = Striped.lock(config.getNumThreads());
    this.collector = new TimelineMetadataCollector<>(serverView, cursorFactories);
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("WatermarkCollector can't start.");
      }
      log.info("Starting WatermarkCollector.");
      try {
        watermarkSink.initialize();
        watermarkSource.initialize();
        int numThreads = Math.min(config.getNumThreads() - 1, Runtime.getRuntime().availableProcessors() - 1);
        if (numThreads > 1) {
          executor = Execs.multiThreaded(numThreads, "WatermarkCollector-%s");
        } else {
          executor = Execs.singleThreaded("WatermarkCollector-%s");
        }

        serverView.registerTimelineCallback(executor, getTimelineCallback());

        if (serverView.isInitialized()) {
          log.info("Segments view initialized.");
          initializeTimelines();
        } else {
          // test: initialized event isn't triggering from TimelineMetadataCollectorServerView, so trying here like broker does
          serverInventoryView.registerSegmentCallback(
              MoreExecutors.sameThreadExecutor(),
              new ServerView.BaseSegmentCallback()
              {
                @Override
                public ServerView.CallbackAction segmentViewInitialized()
                {
                  log.info("Segments inventory view initialized.");
                  initializeTimelines();
                  serviceAnnouncer.announce(self);
                  log.info("WatermarksCollector initialized.");
                  return ServerView.CallbackAction.UNREGISTER;
                }
              },
              // We are not interested in any segment callbacks except view initialization
              Predicates.alwaysFalse()
          );
          log.info("WatermarkCollector started, waiting for segments view to initialize.");
        }

        lifecycleLock.started();
      }
      catch (Exception ex) {
        throw new RE(ex, "WatermarkCollector failed to start!");
      }
      finally {
        lifecycleLock.exitStart();
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStop()) {
        throw new ISE("can't stop.");
      }

      log.info("Stopping WatermarkCollector.");
      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }

      serviceAnnouncer.unannounce(self);
    }
  }

  private TimelineServerView.TimelineCallback getTimelineCallback()
  {
    return new TimelineServerView.TimelineCallback()
    {
      @Override
      public ServerView.CallbackAction timelineInitialized()
      {
        log.info("Segments view initialized.");
        return ServerView.CallbackAction.CONTINUE;
      }

      @Override
      public ServerView.CallbackAction segmentAdded(DruidServerMetadata druidServerMetadata, DataSegment dataSegment)
      {
        log.debug("Segments added for datasource %s.", dataSegment.getDataSource());
        if (initializedTimelinesLifecycleLock.awaitStarted(100, TimeUnit.MILLISECONDS)) {
          traceTimeline(dataSegment.getDataSource());
        } else {
          log.debug("Ignored segment added due to initialization in progress!");
        }
        return ServerView.CallbackAction.CONTINUE;
      }

      @Override
      public ServerView.CallbackAction segmentRemoved(DataSegment dataSegment)
      {
        log.debug("Segment removed");
        if (initializedTimelinesLifecycleLock.awaitStarted(100, TimeUnit.MILLISECONDS)) {
          traceTimeline(dataSegment.getDataSource(), true);
        } else {
          log.debug("Ignored segment removed due to initialization in progress!");
        }
        return ServerView.CallbackAction.CONTINUE;
      }
    };
  }


  /**
   * perform initial trace of timelines for all datasources
   */
  private void initializeTimelines()
  {
    log.info("Initializing watermark collector.");
    synchronized (initializedTimelinesLifecycleLock) {
      if (!initializedTimelinesLifecycleLock.canStart()) {
        log.warn("Already initialized!");
        return;
      }
      try {
        if (config.getReconcileAtStartup()) {
          log.info("Reconciling segments loaded in cluster with watermarks source.");

          for (final String s : serverView.getDataSources()) {
            traceTimeline(s, true);
          }
          log.info("Reconciliation complete.");
        }
        initializedTimelinesLifecycleLock.started();
      }
      finally {
        initializedTimelinesLifecycleLock.exitStart();
      }
    }
  }

  /**
   * Trace the timeline of a datasource starting from the 'anchor' watermark
   *
   * @param dataSource
   */
  private void traceTimeline(String dataSource)
  {
    traceTimeline(dataSource, false);
  }

  /**
   * Trace the timeline of a datasource, either fully or starting from the 'anchor' watermark
   *
   * @param dataSource
   * @param fullTrace
   */
  private void traceTimeline(String dataSource, boolean fullTrace)
  {
    log.debug("Starting timeline trace for '%s'", dataSource);
    final long startNs = System.nanoTime();
    Lock lock = this.datasourceLock.get(dataSource);
    lock.lock();
    try {
      final long lockTimeNs = System.nanoTime() - startNs;

      DateTime initialBatchComplete = watermarkSource.getValue(dataSource, config.getAnchorWatermark());

      DateTime startTime = initialBatchComplete;
      DateTime endTime = DateTimes.nowUtc();

      boolean canCaptureMin = false;
      if (fullTrace || startTime == null) {
        canCaptureMin = true;
        startTime = DateTimes.utc(0);
      }

      log.debug("Tracing timeline for '%s' for interval %s to %s", dataSource, startTime, endTime);
      Interval traceInterval = new Interval(startTime, endTime);

      collector.fold(dataSource, traceInterval, canCaptureMin);

      final long traceTimeNs = System.nanoTime() - startNs;

      // todo: i think this isn't interesting below 1ms, but maybe that's too high?
      if (lockTimeNs > TimeUnit.MILLISECONDS.toNanos(1)) {
        serviceEmitter.emit(
            ServiceMetricEvent
                .builder()
                .setDimension(DruidMetrics.DATASOURCE, dataSource)
                .build(
                    "watermark-collector/lock/time",
                    TimeUnit.NANOSECONDS.toMillis(lockTimeNs)
                )
        );
      }
      serviceEmitter.emit(
          ServiceMetricEvent
              .builder()
              .setDimension(DruidMetrics.DATASOURCE, dataSource)
              .build(
                  "watermark-collector/trace/time",
                  TimeUnit.NANOSECONDS.toMillis(traceTimeNs)
              )
      );

    }
    catch (Exception ex) {
      throw new RE(ex, "Failed to trace timeline for '%s'", dataSource);
    }
    finally {
      lock.unlock();
    }
  }
}
