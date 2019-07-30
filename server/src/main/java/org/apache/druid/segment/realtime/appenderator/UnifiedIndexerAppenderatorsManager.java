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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.plumber.Sink;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Manages Appenderators for the Indexer task execution service, which runs all tasks in a single process.
 *
 * This class keeps two maps:
 * - A per-datasource SinkQuerySegmentWalker (with an associated per-datasource timeline)
 * - A map that associates a taskId with the Appenderator created for that task
 *
 * Appenderators created by this class will use the shared per-datasource SinkQuerySegmentWalkers.
 *
 * The per-datasource SinkQuerySegmentWalkers share a common queryExecutorService.
 */
public class UnifiedIndexerAppenderatorsManager implements AppenderatorsManager
{
  private final ConcurrentHashMap<String, SinkQuerySegmentWalker> datasourceSegmentWalkers = new ConcurrentHashMap<>();

  private final ExecutorService queryExecutorService;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;

  @Inject
  public UnifiedIndexerAppenderatorsManager(
      @Processing ExecutorService queryExecutorService,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats
  )
  {
    this.queryExecutorService = queryExecutorService;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
  }

  @Override
  public Appenderator createRealtimeAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter,
      ExecutorService queryExecutorService,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats
  )
  {
    SinkQuerySegmentWalker segmentWalker = datasourceSegmentWalkers.computeIfAbsent(
        schema.getDataSource(),
        (datasource) -> {
          VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<>(
              String.CASE_INSENSITIVE_ORDER
          );
          SinkQuerySegmentWalker datasourceSegmentWalker = new SinkQuerySegmentWalker(
              schema.getDataSource(),
              sinkTimeline,
              objectMapper,
              emitter,
              conglomerate,
              this.queryExecutorService,
              Preconditions.checkNotNull(this.cache, "cache"),
              this.cacheConfig,
              this.cachePopulatorStats
          );
          return datasourceSegmentWalker;
        }
    );

    Appenderator appenderator = new AppenderatorImpl(
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        segmentAnnouncer,
        segmentWalker,
        indexIO,
        indexMerger,
        cache
    );

    return appenderator;
  }

  @Override
  public Appenderator createOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger
  )
  {
    Appenderator appenderator = Appenderators.createOffline(
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger
    );
    return appenderator;
  }

  @Override
  public void removeAppenderatorForTask(String taskId)
  {
    // nothing to remove presently
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  )
  {
    SinkQuerySegmentWalker segmentWalker = datasourceSegmentWalkers.get(query.getDataSource().toString());
    if (segmentWalker == null) {
      throw new IAE("Could not find segment walker for datasource [%s]", query.getDataSource().toString());
    }
    return segmentWalker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  )
  {
    SinkQuerySegmentWalker segmentWalker = datasourceSegmentWalkers.get(query.getDataSource().toString());
    if (segmentWalker == null) {
      throw new IAE("Could not find segment walker for datasource [%s]", query.getDataSource().toString());
    }
    return segmentWalker.getQueryRunnerForSegments(query, specs);
  }

  @Override
  public boolean shouldTaskMakeNodeAnnouncements()
  {
    return false;
  }
}
