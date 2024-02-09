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
 * This interface defines entities that create and manage potentially multiple {@link Appenderator} instances.
 *
 * The AppenderatorsManager should be used by tasks running in a Peon or an CliIndexer process when it needs
 * an Appenderator.
 *
 * The AppenderatorsManager also provides methods for creating {@link QueryRunner} instances that read the data
 * held by the Appenderators created through the AppenderatorsManager.
 *
 * In later updates, this interface will be used to manage memory usage across multiple Appenderators,
 * useful for the Indexer where all Tasks run in the same process.
 *
 * The methods on AppenderatorsManager can be called by multiple threads.
 *
 * This class provides similar functionality to the {@link org.apache.druid.server.coordination.ServerManager} and
 * {@link org.apache.druid.server.SegmentManager} on the Historical processes.
 */
public interface AppenderatorsManager
{
  /**
   * Creates an Appenderator suited for realtime ingestion. Note that this method's parameters include objects
   * used for query processing.
   */
  Appenderator createRealtimeAppenderatorForTask(
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
  );

  /**
   * Creates an Appenderator suited for batch ingestion.
   */
  Appenderator createOpenSegmentsOfflineAppenderatorForTask(
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
  );

  Appenderator createClosedSegmentsOfflineAppenderatorForTask(
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
  );

  Appenderator createOfflineAppenderatorForTask(
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
  );

  /**
   * Removes any internal Appenderator-tracking state associated with the provided taskId.
   *
   * This method should be called when a task is finished using its Appenderators that were previously created by
   * createRealtimeAppenderatorForTask or createOfflineAppenderatorForTask.
   *
   * The method can be called by the entity managing Tasks when the Tasks finish, such as ThreadingTaskRunner.
   */
  void removeAppenderatorsForTask(String taskId, String dataSource);

  /**
   * Returns a query runner for the given intervals over the Appenderators managed by this AppenderatorsManager.
   */
  <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  );

  /**
   * Returns a query runner for the given segment specs over the Appenderators managed by this AppenderatorsManager.
   */
  <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  );

  /**
   * As AppenderatorsManager implementions are service dependent (i.e., Peons and Indexers have different impls),
   * this method allows Tasks to know whether they should announce themselves as nodes and segment servers
   * to the rest of the cluster.
   *
   * Only Tasks running in Peons (i.e., as separate processes) should make their own individual node announcements.
   */
  boolean shouldTaskMakeNodeAnnouncements();

  /**
   * Shut down the AppenderatorsManager.
   */
  void shutdown();
}
