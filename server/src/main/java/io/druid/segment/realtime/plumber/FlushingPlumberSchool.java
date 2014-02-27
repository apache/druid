/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.Granularity;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Processing;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeDriverConfig;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * This plumber just drops segments at the end of a flush duration instead of handing them off. It is only useful if you want to run
 * a real time node without the rest of the Druid cluster.
 */
public class FlushingPlumberSchool extends RealtimePlumberSchool
{
  private static final EmittingLogger log = new EmittingLogger(FlushingPlumberSchool.class);

  private final Duration flushDuration;

  @JacksonInject
  @NotNull
  private volatile ServiceEmitter emitter;

  @JacksonInject
  @NotNull
  private volatile QueryRunnerFactoryConglomerate conglomerate = null;

  @JacksonInject
  @NotNull
  private volatile DataSegmentAnnouncer segmentAnnouncer = null;

  @JacksonInject
  @NotNull
  @Processing
  private volatile ExecutorService queryExecutorService = null;

  @JsonCreator
  public FlushingPlumberSchool(
      @JsonProperty("flushDuration") Duration flushDuration,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    super(windowPeriod, basePersistDirectory, segmentGranularity);

    this.flushDuration = flushDuration;
  }

  @Override
  public Plumber findPlumber(
      final DataSchema schema,
      final RealtimeDriverConfig config,
      final FireDepartmentMetrics metrics
  )
  {
    verifyState();

    return new FlushingPlumber(
        flushDuration,
        schema,
        config,
        metrics,
        emitter,
        conglomerate,
        segmentAnnouncer,
        queryExecutorService
    );
  }

  private void verifyState()
  {
    Preconditions.checkNotNull(conglomerate, "must specify a queryRunnerFactoryConglomerate to do this action.");
    Preconditions.checkNotNull(segmentAnnouncer, "must specify a segmentAnnouncer to do this action.");
    Preconditions.checkNotNull(emitter, "must specify a serviceEmitter to do this action.");
  }
}
