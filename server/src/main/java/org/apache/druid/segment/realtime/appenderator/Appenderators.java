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
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.concurrent.ExecutorService;

public class Appenderators
{
  public static Appenderator createRealtime(
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
    return new AppenderatorImpl(
        schema,
        config,
        false,
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
            Preconditions.checkNotNull(cache, "cache"),
            cacheConfig,
            cachePopulatorStats
        ),
        indexIO,
        indexMerger,
        cache
    );
  }

  public static Appenderator createOffline(
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
    return new AppenderatorImpl(
        schema,
        config,
        storeCompactionState,
        metrics,
        dataSegmentPusher,
        objectMapper,
        new DataSegmentAnnouncer()
        {
          @Override
          public void announceSegment(DataSegment segment)
          {
            // Do nothing
          }

          @Override
          public void unannounceSegment(DataSegment segment)
          {
            // Do nothing
          }

          @Override
          public void announceSegments(Iterable<DataSegment> segments)
          {
            // Do nothing
          }

          @Override
          public void unannounceSegments(Iterable<DataSegment> segments)
          {
            // Do nothing
          }
        },
        null,
        indexIO,
        indexMerger,
        null
    );
  }
}
