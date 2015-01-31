/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.Granularity;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.FilteredServerView;
import io.druid.guice.annotations.Processing;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Period;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 */
public class RealtimePlumberSchool implements PlumberSchool
{
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentPusher dataSegmentPusher;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final SegmentPublisher segmentPublisher;
  private final FilteredServerView serverView;
  private final ExecutorService queryExecutorService;

  @JsonCreator
  public RealtimePlumberSchool(
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject SegmentPublisher segmentPublisher,
      @JacksonInject FilteredServerView serverView,
      @JacksonInject @Processing ExecutorService executorService
  )
  {
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentAnnouncer = segmentAnnouncer;
    this.segmentPublisher = segmentPublisher;
    this.serverView = serverView;
    this.queryExecutorService = executorService;
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
        queryExecutorService,
        dataSegmentPusher,
        segmentPublisher,
        serverView
    );
  }

  private void verifyState()
  {
    Preconditions.checkNotNull(conglomerate, "must specify a queryRunnerFactoryConglomerate to do this action.");
    Preconditions.checkNotNull(dataSegmentPusher, "must specify a segmentPusher to do this action.");
    Preconditions.checkNotNull(segmentAnnouncer, "must specify a segmentAnnouncer to do this action.");
    Preconditions.checkNotNull(segmentPublisher, "must specify a segmentPublisher to do this action.");
    Preconditions.checkNotNull(serverView, "must specify a serverView to do this action.");
    Preconditions.checkNotNull(emitter, "must specify a serviceEmitter to do this action.");
  }
}
