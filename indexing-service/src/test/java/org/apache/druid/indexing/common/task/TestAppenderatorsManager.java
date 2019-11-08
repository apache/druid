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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
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
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Interval;

import java.util.concurrent.ExecutorService;

public class TestAppenderatorsManager implements AppenderatorsManager
{
  private Appenderator realtimeAppenderator;

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
    realtimeAppenderator = Appenderators.createRealtime(
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger,
        conglomerate,
        segmentAnnouncer,
        emitter,
        queryExecutorService,
        cache,
        cacheConfig,
        cachePopulatorStats
    );
    return realtimeAppenderator;
  }

  @Override
  public Appenderator createOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      boolean storeCompactionState,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger
  )
  {
    return Appenderators.createOffline(
        schema,
        config,
        storeCompactionState,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger
    );
  }

  @Override
  public void removeAppenderatorsForTask(String taskId, String dataSource)
  {
    // nothing to remove
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  )
  {
    if (realtimeAppenderator != null) {
      return realtimeAppenderator.getQueryRunnerForIntervals(query, intervals);
    } else {
      return null;
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  )
  {
    if (realtimeAppenderator != null) {
      return realtimeAppenderator.getQueryRunnerForSegments(query, specs);
    } else {
      return null;
    }
  }

  @Override
  public boolean shouldTaskMakeNodeAnnouncements()
  {
    return true;
  }

  @Override
  public void shutdown()
  {

  }
}
