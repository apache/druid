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
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.logging.log4j.ThreadContext;

import java.io.File;

public class Appenderators
{
  private static final String THREAD_CONTEXT_TASK_LOG_FILE = "task.log.file";
  private static final String THREAD_CONTEXT_TASK_ID = "task.log.id";

  public static Appenderator createRealtime(
      SegmentLoaderConfig segmentLoaderConfig,
      String id,
      DataSchema schema,
      AppenderatorConfig config,
      SegmentGenerationMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter,
      QueryProcessingPool queryProcessingPool,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      PolicyEnforcer policyEnforcer,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      TaskLockCallback taskLockCallback
  )
  {
    return new StreamAppenderator(
        segmentLoaderConfig,
        id,
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        segmentAnnouncer,
        new SinkQuerySegmentWalker(
            schema.getDataSource(),
            new VersionedIntervalTimeline<>(
                String.CASE_INSENSITIVE_ORDER
            ),
            objectMapper,
            emitter,
            conglomerate,
            queryProcessingPool,
            Preconditions.checkNotNull(cache, "cache"),
            cacheConfig,
            cachePopulatorStats,
            policyEnforcer
        ),
        indexIO,
        indexMerger,
        cache,
        rowIngestionMeters,
        parseExceptionHandler,
        centralizedDatasourceSchemaConfig,
        taskLockCallback
    );
  }

  public static Appenderator createBatch(
      String id,
      DataSchema schema,
      AppenderatorConfig config,
      SegmentGenerationMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    // Use newest, slated to be the permanent batch appenderator but for now keeping it as a non-default
    // option due to risk mitigation...will become default and the two other appenderators eliminated when
    // stability is proven...
    return new BatchAppenderator(
        id,
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        indexIO,
        indexMerger,
        rowIngestionMeters,
        parseExceptionHandler,
        centralizedDatasourceSchemaConfig
    );
  }

  /**
   * Sets the thread context variables {@code task.log.id} and {@code task.log.file}
   * used to route logs of task threads on Indexers to separate log files.
   */
  public static void setTaskThreadContextForIndexers(String taskId, File logFile)
  {
    ThreadContext.put(THREAD_CONTEXT_TASK_ID, taskId);
    ThreadContext.put(THREAD_CONTEXT_TASK_LOG_FILE, logFile.getAbsolutePath());
  }

  /**
   * Clears the thread context variables {@code task.log.id} and {@code task.log.file}
   * used to route logs of task threads on Indexers to separate log files.
   */
  public static void clearTaskThreadContextForIndexers()
  {
    ThreadContext.remove(THREAD_CONTEXT_TASK_LOG_FILE);
    ThreadContext.remove(THREAD_CONTEXT_TASK_ID);
  }
}
