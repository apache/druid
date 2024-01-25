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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.partition.ShardSpec;

import java.io.File;

public class DefaultRealtimeAppenderatorFactory implements AppenderatorFactory
{
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final QueryProcessingPool queryProcessingPool;
  private final JoinableFactory joinableFactory;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;

  public DefaultRealtimeAppenderatorFactory(
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject QueryProcessingPool queryProcessingPool,
      @JacksonInject JoinableFactory joinableFactory,
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject @Json ObjectMapper jsonMapper,
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
    this.queryProcessingPool = queryProcessingPool;
    this.joinableFactory = joinableFactory;
    this.dataSegmentPusher = dataSegmentPusher;
    this.jsonMapper = jsonMapper;
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
    final RowIngestionMeters rowIngestionMeters = new NoopRowIngestionMeters();
    return Appenderators.createRealtime(
        null,
        schema.getDataSource(),
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
        jsonMapper,
        indexIO,
        indexMerger,
        conglomerate,
        segmentAnnouncer,
        emitter,
        queryProcessingPool,
        cache,
        cacheConfig,
        cachePopulatorStats,
        rowIngestionMeters,
        new ParseExceptionHandler(
            rowIngestionMeters,
            false,
            config.isReportParseExceptions() ? 0 : Integer.MAX_VALUE,
            0
        ),
        true,
        null
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
