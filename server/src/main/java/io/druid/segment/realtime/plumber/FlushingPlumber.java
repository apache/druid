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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.Granularity;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class FlushingPlumber extends RealtimePlumber
{
  private static final EmittingLogger log = new EmittingLogger(FlushingPlumber.class);

  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final Duration flushDuration;

  private volatile ScheduledExecutorService flushScheduledExec = null;
  private volatile boolean stopped = false;

  public FlushingPlumber(
      Duration flushDuration,
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      IndexMerger indexMerger,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      ObjectMapper objectMapper

  )
  {
    super(
        schema,
        config,
        metrics,
        emitter,
        conglomerate,
        segmentAnnouncer,
        queryExecutorService,
        null,
        null,
        null,
        indexMerger,
        indexIO,
        cache,
        cacheConfig,
        objectMapper
    );

    this.flushDuration = flushDuration;
    this.schema = schema;
    this.config = config;
  }

  @Override
  public Object startJob()
  {
    log.info("Starting job for %s", getSchema().getDataSource());

    computeBaseDir(getSchema()).mkdirs();
    initializeExecutors();

    if (flushScheduledExec == null) {
      flushScheduledExec = Execs.scheduledSingleThreaded("flushing_scheduled_%d");
    }

    Object retVal = bootstrapSinksFromDisk();
    startFlushThread();
    return retVal;
  }

  protected void flushAfterDuration(final long truncatedTime, final Sink sink)
  {
    log.info(
        "Abandoning segment %s at %s",
        sink.getSegment().getIdentifier(),
        new DateTime().plusMillis((int) flushDuration.getMillis())
    );

    ScheduledExecutors.scheduleWithFixedDelay(
        flushScheduledExec,
        flushDuration,
        new Callable<ScheduledExecutors.Signal>()
        {
          @Override
          public ScheduledExecutors.Signal call() throws Exception
          {
            log.info("Abandoning segment %s", sink.getSegment().getIdentifier());
            abandonSegment(truncatedTime, sink);
            return ScheduledExecutors.Signal.STOP;
          }
        }
    );
  }

  private void startFlushThread()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final DateTime truncatedNow = segmentGranularity.truncate(new DateTime());
    final long windowMillis = config.getWindowPeriod().toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(
                System.currentTimeMillis(),
                schema.getGranularitySpec().getSegmentGranularity().increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            flushScheduledExec,
            new Duration(
                System.currentTimeMillis(),
                schema.getGranularitySpec().getSegmentGranularity().increment(truncatedNow).getMillis() + windowMillis
            ),
            new Duration(truncatedNow, segmentGranularity.increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-flusher-%d",
                    getSchema().getDataSource(),
                    getConfig().getShardSpec().getPartitionNum()
                )
            )
            {
              @Override
              public ScheduledExecutors.Signal doCall()
              {
                if (stopped) {
                  log.info("Stopping flusher thread");
                  return ScheduledExecutors.Signal.STOP;
                }

                long minTimestamp = segmentGranularity.truncate(
                    getRejectionPolicy().getCurrMaxTime().minus(windowMillis)
                ).getMillis();

                List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
                for (Map.Entry<Long, Sink> entry : getSinks().entrySet()) {
                  final Long intervalStart = entry.getKey();
                  if (intervalStart < minTimestamp) {
                    log.info("Adding entry[%s] to flush.", entry);
                    sinksToPush.add(entry);
                  }
                }

                for (final Map.Entry<Long, Sink> entry : sinksToPush) {
                  flushAfterDuration(entry.getKey(), entry.getValue());
                }

                if (stopped) {
                  log.info("Stopping flusher thread");
                  return ScheduledExecutors.Signal.STOP;
                } else {
                  return ScheduledExecutors.Signal.REPEAT;
                }
              }
            }
        );
  }

  @Override
  public void finishJob()
  {
    log.info("Stopping job");

    for (final Map.Entry<Long, Sink> entry : getSinks().entrySet()) {
      abandonSegment(entry.getKey(), entry.getValue());
    }
    shutdownExecutors();

    if (flushScheduledExec != null) {
      flushScheduledExec.shutdown();
    }

    stopped = true;
  }
}
