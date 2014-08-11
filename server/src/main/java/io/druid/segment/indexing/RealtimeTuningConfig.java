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
import com.google.common.io.Files;
import io.druid.segment.realtime.plumber.IntervalStartVersioningPolicy;
import io.druid.segment.realtime.plumber.RejectionPolicyFactory;
import io.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import java.io.File;

/**
 */
public class RealtimeTuningConfig implements TuningConfig
{
  private static final int defaultMaxRowsInMemory = 500000;
  private static final Period defaultIntermediatePersistPeriod = new Period("PT10M");
  private static final Period defaultWindowPeriod = new Period("PT10M");
  private static final File defaultBasePersistDirectory = Files.createTempDir();
  private static final VersioningPolicy defaultVersioningPolicy = new IntervalStartVersioningPolicy();
  private static final RejectionPolicyFactory defaultRejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();
  private static final int defaultMaxPendingPersists = 0;
  private static final ShardSpec defaultShardSpec = new NoneShardSpec();

  // Might make sense for this to be a builder
  public static RealtimeTuningConfig makeDefaultTuningConfig()
  {
    return new RealtimeTuningConfig(
        defaultMaxRowsInMemory,
        defaultIntermediatePersistPeriod,
        defaultWindowPeriod,
        defaultBasePersistDirectory,
        defaultVersioningPolicy,
        defaultRejectionPolicyFactory,
        defaultMaxPendingPersists,
        defaultShardSpec
    );
  }

  private final int maxRowsInMemory;
  private final Period intermediatePersistPeriod;
  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final VersioningPolicy versioningPolicy;
  private final RejectionPolicyFactory rejectionPolicyFactory;
  private final int maxPendingPersists;
  private final ShardSpec shardSpec;

  @JsonCreator
  public RealtimeTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("versioningPolicy") VersioningPolicy versioningPolicy,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicyFactory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec
  )
  {
    this.maxRowsInMemory = maxRowsInMemory == null ? defaultMaxRowsInMemory : maxRowsInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaultIntermediatePersistPeriod
                                     : intermediatePersistPeriod;
    this.windowPeriod = windowPeriod == null ? defaultWindowPeriod : windowPeriod;
    this.basePersistDirectory = basePersistDirectory == null ? defaultBasePersistDirectory : basePersistDirectory;
    this.versioningPolicy = versioningPolicy == null ? defaultVersioningPolicy : versioningPolicy;
    this.rejectionPolicyFactory = rejectionPolicyFactory == null
                                  ? defaultRejectionPolicyFactory
                                  : rejectionPolicyFactory;
    this.maxPendingPersists = maxPendingPersists == null ? defaultMaxPendingPersists : maxPendingPersists;
    this.shardSpec = shardSpec == null ? defaultShardSpec : shardSpec;
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
    return rejectionPolicyFactory;
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

  public RealtimeTuningConfig withVersioningPolicy(VersioningPolicy policy)
  {
    return new RealtimeTuningConfig(
        maxRowsInMemory,
        intermediatePersistPeriod,
        windowPeriod,
        basePersistDirectory,
        policy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec
    );
  }

  public RealtimeTuningConfig withBasePersistDirectory(File dir)
  {
    return new RealtimeTuningConfig(
        maxRowsInMemory,
        intermediatePersistPeriod,
        windowPeriod,
        dir,
        versioningPolicy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec
    );
  }
}
