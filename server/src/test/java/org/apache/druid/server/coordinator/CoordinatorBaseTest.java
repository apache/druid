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

package org.apache.druid.server.coordinator;

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverDropRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CoordinatorBaseTest
{
  /**
   * Datasource names used in tests.
   */
  public static class DS
  {
    public static final String WIKI = "wiki";
    public static final String KOALA = "koala";
    public static final String BROADCAST = "broadcast";
  }

  /**
   * Server tier names used in tests.
   */
  public static class Tier
  {
    public static final String T1 = "tier_t1";
    public static final String T2 = "tier_t2";
    public static final String T3 = "tier_t3";
  }

  /**
   * Builder for a broadcast rule.
   */
  public static class Broadcast
  {
    public static Rule forever()
    {
      return new ForeverBroadcastDistributionRule();
    }
  }

  /**
   * Builder for a {@link LoadRule}.
   */
  public static class Load
  {
    private final Map<String, Integer> tieredReplicants = new HashMap<>();

    public static Load on(String tier, int numReplicas)
    {
      return new Load().andOn(tier, numReplicas);
    }

    public Load andOn(String tier, int numReplicas)
    {
      tieredReplicants.put(tier, numReplicas);
      return this;
    }

    public LoadRule forever()
    {
      return new ForeverLoadRule(tieredReplicants, null);
    }

    public LoadRule forInterval(String interval)
    {
      return new IntervalLoadRule(Intervals.of(interval), tieredReplicants, null);
    }
  }

  /**
   * Builder for a drop rule.
   */
  protected static class Drop
  {
    public static Rule forever()
    {
      return new ForeverDropRule();
    }

    public static Rule forInterval(String interval)
    {
      return new IntervalDropRule(Intervals.of(interval));
    }
  }

  /**
   * Builder for a {@link ServerHolder} of type
   * {@link ServerType#HISTORICAL}.
   */
  public static class Historical
  {
    private static final AtomicLong SERVER_ID = new AtomicLong(0);

    private final Set<DataSegment> segments = new HashSet<>();

    /**
     * Default size of 10GB.
     */
    private long size = 10L << 30;
    private boolean decommissioning = false;

    public Historical ofSizeInGb(long sizeInGb)
    {
      this.size = sizeInGb << 30;
      return this;
    }

    public Historical inDecommissioningMode()
    {
      this.decommissioning = true;
      return this;
    }

    public Historical withSegments(DataSegment... segments)
    {
      return withSegments(Arrays.asList(segments));
    }

    public Historical withSegments(Collection<DataSegment> segments)
    {
      this.segments.addAll(segments);
      return this;
    }

    public DruidServer in(String tier)
    {
      String name = "hist_" + SERVER_ID.incrementAndGet();

      final DruidServer server = new DruidServer(name, name, null, size, ServerType.HISTORICAL, tier, 1);
      for (DataSegment segment : segments) {
        server.addDataSegment(segment);
      }

      return server;
    }

    public ImmutableDruidServer asImmutable()
    {
      return in().toImmutableDruidServer();
    }

    public ServerHolder asHolder()
    {
      return new ServerHolder(
          asImmutable(),
          new TestLoadQueuePeon(),
          decommissioning
      );
    }
  }

}
