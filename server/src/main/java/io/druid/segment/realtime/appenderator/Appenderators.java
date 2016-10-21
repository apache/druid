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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;

import java.io.IOException;
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
      CacheConfig cacheConfig
  )
  {
    return new AppenderatorImpl(
        schema,
        config,
        metrics,
        dataSegmentPusher,
        objectMapper,
        conglomerate,
        segmentAnnouncer,
        emitter,
        queryExecutorService,
        indexIO,
        indexMerger,
        cache,
        cacheConfig
    );
  }

  public static Appenderator createOffline(
      DataSchema schema,
      AppenderatorConfig config,
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
        metrics,
        dataSegmentPusher,
        objectMapper,
        null,
        new DataSegmentAnnouncer()
        {
          @Override
          public void announceSegment(DataSegment segment) throws IOException
          {
            // Do nothing
          }

          @Override
          public void unannounceSegment(DataSegment segment) throws IOException
          {
            // Do nothing
          }

          @Override
          public void announceSegments(Iterable<DataSegment> segments) throws IOException
          {
            // Do nothing
          }

          @Override
          public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
          {
            // Do nothing
          }

          @Override
          public boolean isAnnounced(DataSegment segment)
          {
            return false;
          }
        },
        null,
        null,
        indexIO,
        indexMerger,
        null,
        null
    );
  }


}
