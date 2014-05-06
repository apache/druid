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
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Processing;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexGranularity;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.Schema;
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
public class FlushingPlumberSchool implements PlumberSchool
{
  private static final EmittingLogger log = new EmittingLogger(FlushingPlumberSchool.class);

  private final Duration flushDuration;
  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final IndexGranularity segmentGranularity;
  private final int maxPendingPersists;

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

  private volatile VersioningPolicy versioningPolicy = null;
  private volatile RejectionPolicyFactory rejectionPolicyFactory = null;

  @JsonCreator
  public FlushingPlumberSchool(
      @JsonProperty("flushDuration") Duration flushDuration,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("segmentGranularity") IndexGranularity segmentGranularity
  )
  {
    this.flushDuration = flushDuration;
    this.windowPeriod = windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.segmentGranularity = segmentGranularity;
    this.versioningPolicy = new IntervalStartVersioningPolicy();
    this.rejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();
    // Workaround for Jackson issue where if maxPendingPersists is null, all JacksonInjects fail
    this.maxPendingPersists = RealtimePlumberSchool.DEFAULT_MAX_PENDING_PERSISTS;

    Preconditions.checkNotNull(flushDuration, "FlushingPlumberSchool requires a flushDuration.");
    Preconditions.checkNotNull(windowPeriod, "FlushingPlumberSchool requires a windowPeriod.");
    Preconditions.checkNotNull(basePersistDirectory, "FlushingPlumberSchool requires a basePersistDirectory.");
    Preconditions.checkNotNull(segmentGranularity, "FlushingPlumberSchool requires a segmentGranularity.");
  }

  @Override

  public Plumber findPlumber(final Schema schema, final FireDepartmentMetrics metrics)
  {
    verifyState();

    final RejectionPolicy rejectionPolicy = rejectionPolicyFactory.create(windowPeriod);
    log.info("Creating plumber using rejectionPolicy[%s]", rejectionPolicy);

    return new FlushingPlumber(
        flushDuration,
        windowPeriod,
        basePersistDirectory,
        segmentGranularity,
        schema,
        metrics,
        rejectionPolicy,
        emitter,
        conglomerate,
        segmentAnnouncer,
        queryExecutorService,
        versioningPolicy,
        maxPendingPersists
    );
  }

  private void verifyState()
  {
    Preconditions.checkNotNull(conglomerate, "must specify a queryRunnerFactoryConglomerate to do this action.");
    Preconditions.checkNotNull(segmentAnnouncer, "must specify a segmentAnnouncer to do this action.");
    Preconditions.checkNotNull(emitter, "must specify a serviceEmitter to do this action.");
  }
}
