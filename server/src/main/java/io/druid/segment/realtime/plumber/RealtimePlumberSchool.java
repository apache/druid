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
import io.druid.client.ServerView;
import io.druid.guice.annotations.Processing;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.IndexGranularity;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.Schema;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 */
public class RealtimePlumberSchool implements PlumberSchool
{
  public static final int DEFAULT_MAX_PENDING_PERSISTS = 0;

  private static final EmittingLogger log = new EmittingLogger(RealtimePlumberSchool.class);

  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final IndexGranularity segmentGranularity;

  @JacksonInject
  @NotNull
  private volatile ServiceEmitter emitter;

  @JacksonInject
  @NotNull
  private volatile QueryRunnerFactoryConglomerate conglomerate = null;

  @JacksonInject
  @NotNull
  private volatile DataSegmentPusher dataSegmentPusher = null;

  @JacksonInject
  @NotNull
  private volatile DataSegmentAnnouncer segmentAnnouncer = null;

  @JacksonInject
  @NotNull
  private volatile SegmentPublisher segmentPublisher = null;

  @JacksonInject
  @NotNull
  private volatile ServerView serverView = null;

  @JacksonInject
  @NotNull
  @Processing
  private volatile ExecutorService queryExecutorService = null;

  private volatile int maxPendingPersists;
  private volatile VersioningPolicy versioningPolicy = null;
  private volatile RejectionPolicyFactory rejectionPolicyFactory = null;

  @JsonCreator
  public RealtimePlumberSchool(
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("segmentGranularity") IndexGranularity segmentGranularity
  )
  {
    this.windowPeriod = windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.segmentGranularity = segmentGranularity;
    this.versioningPolicy = new IntervalStartVersioningPolicy();
    this.rejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();
    // Workaround for Jackson issue where if maxPendingPersists is null, all JacksonInjects fail
    this.maxPendingPersists = RealtimePlumberSchool.DEFAULT_MAX_PENDING_PERSISTS;

    Preconditions.checkNotNull(windowPeriod, "RealtimePlumberSchool requires a windowPeriod.");
    Preconditions.checkNotNull(basePersistDirectory, "RealtimePlumberSchool requires a basePersistDirectory.");
    Preconditions.checkNotNull(segmentGranularity, "RealtimePlumberSchool requires a segmentGranularity.");
  }

  @JsonProperty("versioningPolicy")
  public void setVersioningPolicy(VersioningPolicy versioningPolicy)
  {
    this.versioningPolicy = versioningPolicy;
  }

  @JsonProperty("rejectionPolicy")
  public void setRejectionPolicyFactory(RejectionPolicyFactory factory)
  {
    this.rejectionPolicyFactory = factory;
  }

  @JsonProperty("maxPendingPersists")
  public void setDefaultMaxPendingPersists(int maxPendingPersists)
  {
    this.maxPendingPersists = maxPendingPersists;
  }

  public void setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
  }

  public void setConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
  }

  public void setDataSegmentPusher(DataSegmentPusher dataSegmentPusher)
  {
    this.dataSegmentPusher = dataSegmentPusher;
  }

  public void setSegmentAnnouncer(DataSegmentAnnouncer segmentAnnouncer)
  {
    this.segmentAnnouncer = segmentAnnouncer;
  }

  public void setSegmentPublisher(SegmentPublisher segmentPublisher)
  {
    this.segmentPublisher = segmentPublisher;
  }

  public void setServerView(ServerView serverView)
  {
    this.serverView = serverView;
  }

  public void setQueryExecutorService(ExecutorService executorService)
  {
    this.queryExecutorService = executorService;
  }

  @Override
  public Plumber findPlumber(final Schema schema, final FireDepartmentMetrics metrics)
  {
    verifyState();

    final RejectionPolicy rejectionPolicy = rejectionPolicyFactory.create(windowPeriod);
    log.info("Creating plumber using rejectionPolicy[%s]", rejectionPolicy);

    return new RealtimePlumber(
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
        dataSegmentPusher,
        segmentPublisher,
        serverView,
        maxPendingPersists
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
