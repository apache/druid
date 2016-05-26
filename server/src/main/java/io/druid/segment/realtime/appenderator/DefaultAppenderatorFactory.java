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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;

import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.guice.annotations.Processing;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.partition.ShardSpec;

import java.io.File;
import java.util.concurrent.ExecutorService;

public class DefaultAppenderatorFactory implements AppenderatorFactory
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

  public DefaultAppenderatorFactory(
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject @Processing ExecutorService queryExecutorService,
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject IndexIO indexIO,
      @JacksonInject IndexMerger indexMerger,
      @JacksonInject Cache cache,
      @JacksonInject CacheConfig cacheConfig
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
        cacheConfig
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
