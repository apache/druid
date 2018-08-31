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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.partition.ShardSpec;

import java.io.File;
import java.util.concurrent.ExecutorService;

public class DefaultRealtimeAppenderatorFactory implements AppenderatorFactory
{
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ExecutorService queryExecutorService;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;

  public DefaultRealtimeAppenderatorFactory(
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject @Processing ExecutorService queryExecutorService,
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject IndexIO indexIO,
      @JacksonInject IndexMerger indexMerger,
      @JacksonInject Cache cache,
      @JacksonInject CacheConfig cacheConfig,
      @JacksonInject CachePopulatorStats cachePopulatorStats
  )
  {
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.queryExecutorService = queryExecutorService;
    this.dataSegmentPusher = dataSegmentPusher;
    this.objectMapper = objectMapper;
    this.indexIO = indexIO;
    this.indexMerger = indexMerger;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
  }

  @Override
  public Appenderator build(
      final DataSchema schema,
      final RealtimeTuningConfig config,
      final FireDepartmentMetrics metrics
  )
  {
    return Appenderators.createRealtime(
        schema,
        config.withBasePersistDirectory(
            makeBasePersistSubdirectory(
                config.getBasePersistDirectory(),
                schema.getDataSource(),
                config.getShardSpec()
            )
        ),
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
  }

  private static File makeBasePersistSubdirectory(
      final File basePersistDirectory,
      final String dataSource,
      final ShardSpec shardSpec
  )
  {
    final File dataSourceDirectory = new File(basePersistDirectory, dataSource);
    return new File(dataSourceDirectory, String.valueOf(shardSpec.getPartitionNum()));
  }
}
