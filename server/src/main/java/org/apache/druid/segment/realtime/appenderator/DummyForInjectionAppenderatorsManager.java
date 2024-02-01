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
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Interval;

/**
 * This implementation is needed because Overlords and MiddleManagers operate on Task objects which
 * can require an AppenderatorsManager to be injected.
 *
 * The methods of this implementation throw exceptions because the Overlord/MM should never be calling
 * the AppenderatorsManager.
 */
public class DummyForInjectionAppenderatorsManager implements AppenderatorsManager
{
  private static final String ERROR_MSG =
      "AppenderatorsManager methods should only called by services that run tasks directly.";

  @Override
  public Appenderator createRealtimeAppenderatorForTask(
      SegmentLoaderConfig segmentLoaderConfig,
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
      QueryProcessingPool queryProcessingPool,
      JoinableFactory joinableFactory,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    throw new UOE(ERROR_MSG);
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
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates
  )
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public Appenderator createOpenSegmentsOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates
  )
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public Appenderator createClosedSegmentsOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates
  )
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public void removeAppenderatorsForTask(String taskId, String dataSource)
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  )
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  )
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public boolean shouldTaskMakeNodeAnnouncements()
  {
    throw new UOE(ERROR_MSG);
  }

  @Override
  public void shutdown()
  {
    throw new UOE(ERROR_MSG);
  }
}
