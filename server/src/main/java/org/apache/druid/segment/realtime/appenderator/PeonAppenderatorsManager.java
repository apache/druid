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
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Interval;

import java.util.concurrent.ExecutorService;

/**
 * Manages Appenderators for tasks running within a CliPeon process.
 *
 * It provides the ability to create a realtime appenderator or multiple batch appenderators,
 * and serves queries on the realtime appenderator.
 *
 * The implementation contains sanity checks that throw errors if more than one realtime appenderator is created,
 * or if a task tries to create both realtime and batch appenderators. These checks can be adjusted if these
 * assumptions are no longer true.
 *
 * Because the peon is a separate process that will terminate after task completion, this implementation
 * relies on process shutdown for resource cleanup.
 */
public class PeonAppenderatorsManager implements AppenderatorsManager
{
  private Appenderator realtimeAppenderator;
  private Appenderator batchAppenderator;

  @Override
  public Appenderator createRealtimeAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper jsonMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter,
      ExecutorService queryExecutorService,
      JoinableFactory joinableFactory,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats
  )
  {
    if (realtimeAppenderator != null) {
      throw new ISE("A realtime appenderator was already created for this peon's task.");
    } else if (batchAppenderator != null) {
      throw new ISE("A batch appenderator was already created for this peon's task.");
    } else {
      realtimeAppenderator = Appenderators.createRealtime(
          taskId,
          schema,
          config,
          metrics,
          dataSegmentPusher,
          jsonMapper,
          indexIO,
          indexMerger,
          conglomerate,
          segmentAnnouncer,
          emitter,
          queryExecutorService,
          joinableFactory,
          cache,
          cacheConfig,
          cachePopulatorStats
      );
    }
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
    // CompactionTask does run multiple sub-IndexTasks, so we allow multiple batch appenderators
    if (realtimeAppenderator != null) {
      throw new ISE("A realtime appenderator was already created for this peon's task.");
    } else {
      batchAppenderator = Appenderators.createOffline(
          taskId,
          schema,
          config,
          storeCompactionState,
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          indexMerger
      );
      return batchAppenderator;
    }
  }

  @Override
  public void removeAppenderatorsForTask(String taskId, String dataSource)
  {
    // the peon only runs one task, and the process will shutdown later, don't need to do anything
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  )
  {
    if (realtimeAppenderator == null) {
      throw new ISE("Was asked for a query runner but realtimeAppenderator was null!");
    } else {
      return realtimeAppenderator.getQueryRunnerForIntervals(query, intervals);
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  )
  {
    if (realtimeAppenderator == null) {
      throw new ISE("Was asked for a query runner but realtimeAppenderator was null!");
    } else {
      return realtimeAppenderator.getQueryRunnerForSegments(query, specs);
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
    // nothing to shut down
  }
}
