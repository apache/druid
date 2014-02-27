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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.realtime.plumber.IntervalStartVersioningPolicy;
import io.druid.segment.realtime.plumber.RealtimePlumberSchool;
import io.druid.segment.realtime.plumber.RejectionPolicyFactory;
import io.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import java.io.File;

/**
 */
public class RealtimeDriverConfig implements DriverConfig
{
  private final int maxRowsInMemory;
  private final Period intermediatePersistPeriod;
  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final VersioningPolicy versioningPolicy;
  private final RejectionPolicyFactory rejectionPolicy;
  private final int maxPendingPersists;
  private final ShardSpec shardSpec;

  @JsonCreator
  public RealtimeDriverConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("versioningPolicy") VersioningPolicy versioningPolicy,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicy,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec
  )
  {
    this.maxRowsInMemory = maxRowsInMemory == null ? 500000 : maxRowsInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? new Period("PT10M")
                                     : intermediatePersistPeriod;
    this.windowPeriod = windowPeriod == null ? this.intermediatePersistPeriod : windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.versioningPolicy = versioningPolicy == null ? new IntervalStartVersioningPolicy() : versioningPolicy;
    this.rejectionPolicy = rejectionPolicy == null ? new ServerTimeRejectionPolicyFactory() : rejectionPolicy;
    this.maxPendingPersists = maxPendingPersists == null
                              ? RealtimePlumberSchool.DEFAULT_MAX_PENDING_PERSISTS
                              : maxPendingPersists;
    this.shardSpec = shardSpec == null ? new NoneShardSpec() : shardSpec;
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @JsonProperty
  public Period getWindowPeriod()
  {
    return windowPeriod;
  }

  @JsonProperty
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @JsonProperty
  public VersioningPolicy getVersioningPolicy()
  {
    return versioningPolicy;
  }

  @JsonProperty
  public RejectionPolicyFactory getRejectionPolicyFactory()
  {
    return rejectionPolicy;
  }

  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

  public RealtimeDriverConfig withVersioningPolicy(VersioningPolicy policy)
  {
    return new RealtimeDriverConfig(
        maxRowsInMemory,
        intermediatePersistPeriod,
        windowPeriod,
        basePersistDirectory,
        policy,
        rejectionPolicy,
        maxPendingPersists,
        shardSpec
    );
  }
}
