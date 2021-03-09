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
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.concurrent.ExecutorService;

public class Appenderators
{
  public static Appenderator createRealtime(
      String id,
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
      JoinableFactory joinableFactory,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    return new AppenderatorImpl(
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
            queryExecutorService,
            joinableFactory,
            Preconditions.checkNotNull(cache, "cache"),
            cacheConfig,
            cachePopulatorStats
        ),
        indexIO,
        indexMerger,
        cache,
        rowIngestionMeters,
        parseExceptionHandler
    );
  }

  public static Appenderator createOffline(
      String id,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    return new AppenderatorImpl(
        id,
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        new NoopDataSegmentAnnouncer(),
        null,
        indexIO,
        indexMerger,
        null,
        rowIngestionMeters,
        parseExceptionHandler
    );
  }
}
