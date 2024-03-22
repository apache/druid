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

package org.apache.druid.segment.realtime.plumber;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;

/**
 *
 */
public class RealtimePlumberSchool implements PlumberSchool
{
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentPusher dataSegmentPusher;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final SegmentPublisher segmentPublisher;
  private final SegmentHandoffNotifierFactory handoffNotifierFactory;
  private final QueryProcessingPool queryProcessingPool;
  private final JoinableFactory joinableFactory;
  private final IndexMergerV9 indexMergerV9;
  private final IndexIO indexIO;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final ObjectMapper objectMapper;

  @JsonCreator
  public RealtimePlumberSchool(
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject SegmentPublisher segmentPublisher,
      @JacksonInject SegmentHandoffNotifierFactory handoffNotifierFactory,
      @JacksonInject QueryProcessingPool queryProcessingPool,
      @JacksonInject JoinableFactory joinableFactory,
      @JacksonInject IndexMergerV9 indexMergerV9,
      @JacksonInject IndexIO indexIO,
      @JacksonInject Cache cache,
      @JacksonInject CacheConfig cacheConfig,
      @JacksonInject CachePopulatorStats cachePopulatorStats,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentAnnouncer = segmentAnnouncer;
    this.segmentPublisher = segmentPublisher;
    this.handoffNotifierFactory = handoffNotifierFactory;
    this.queryProcessingPool = queryProcessingPool;
    this.joinableFactory = joinableFactory;
    this.indexMergerV9 = Preconditions.checkNotNull(indexMergerV9, "Null IndexMergerV9");
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");

    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
    this.objectMapper = objectMapper;
  }

  @Override
  public Plumber findPlumber(
      final DataSchema schema,
      final RealtimeTuningConfig config,
      final FireDepartmentMetrics metrics
  )
  {
    verifyState();

    return new RealtimePlumber(
        schema,
        config,
        metrics,
        emitter,
        conglomerate,
        segmentAnnouncer,
        queryProcessingPool,
        dataSegmentPusher,
        segmentPublisher,
        handoffNotifierFactory.createSegmentHandoffNotifier(schema.getDataSource()),
        indexMergerV9,
        indexIO,
        cache,
        cacheConfig,
        cachePopulatorStats,
        objectMapper
    );
  }

  private void verifyState()
  {
    Preconditions.checkNotNull(conglomerate, "must specify a queryRunnerFactoryConglomerate to do this action.");
    Preconditions.checkNotNull(dataSegmentPusher, "must specify a segmentPusher to do this action.");
    Preconditions.checkNotNull(segmentAnnouncer, "must specify a segmentAnnouncer to do this action.");
    Preconditions.checkNotNull(segmentPublisher, "must specify a segmentPublisher to do this action.");
    Preconditions.checkNotNull(handoffNotifierFactory, "must specify a handoffNotifierFactory to do this action.");
    Preconditions.checkNotNull(emitter, "must specify a serviceEmitter to do this action.");
  }
}
