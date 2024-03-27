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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverDropRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.rules.Rule;

import java.util.HashMap;
import java.util.Map;

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
}
