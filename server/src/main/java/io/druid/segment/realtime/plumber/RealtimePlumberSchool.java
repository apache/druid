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

  // Backwards compatible
  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final Granularity segmentGranularity;
  private final VersioningPolicy versioningPolicy;
  private final RejectionPolicyFactory rejectionPolicyFactory;
  private final int maxPendingPersists;

  @JsonCreator
  public RealtimePlumberSchool(
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject QueryRunnerFactoryConglomerate conglomerate,
      @JacksonInject DataSegmentPusher dataSegmentPusher,
      @JacksonInject DataSegmentAnnouncer segmentAnnouncer,
      @JacksonInject SegmentPublisher segmentPublisher,
      @JacksonInject FilteredServerView serverView,
      @JacksonInject @Processing ExecutorService executorService,
      // Backwards compatible
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("versioningPolicy") VersioningPolicy versioningPolicy,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicy,
      @JsonProperty("rejectionPolicyFactory") RejectionPolicyFactory rejectionPolicyFactory,
      @JsonProperty("maxPendingPersists") int maxPendingPersists
  )
  {
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentAnnouncer = segmentAnnouncer;
    this.segmentPublisher = segmentPublisher;
    this.serverView = serverView;
    this.queryExecutorService = executorService;
    this.windowPeriod = windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.segmentGranularity = segmentGranularity;
    this.versioningPolicy = versioningPolicy;
    this.rejectionPolicyFactory = (rejectionPolicy == null) ? rejectionPolicyFactory : rejectionPolicy;
    this.maxPendingPersists = maxPendingPersists;
  }

  @Deprecated
  public Period getWindowPeriod()
  {
    return windowPeriod;
  }

  @Deprecated
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @Override
  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Deprecated
  public VersioningPolicy getVersioningPolicy()
  {
    return versioningPolicy;
  }

  @Deprecated
  public RejectionPolicyFactory getRejectionPolicyFactory()
  {
    return rejectionPolicyFactory;
  }

  @Deprecated
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
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
