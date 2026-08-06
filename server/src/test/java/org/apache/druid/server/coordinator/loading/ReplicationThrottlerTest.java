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

package org.apache.druid.server.coordinator.loading;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReplicationThrottlerTest
{
  private static final String TIER_1 = "t1";
  private static final String TIER_2 = "t2";

  @Test
  public void testTierDoesNotViolateThrottleLimit()
  {
    final int replicationThrottleLimit = 10;
    ReplicationThrottler throttler = new ReplicationThrottler(
        ImmutableMap.of(),
        replicationThrottleLimit
    );

    // Verify that both the tiers can be assigned replicas upto the limit
    for (int i = 0; i < replicationThrottleLimit; ++i) {
      Assertions.assertFalse(throttler.isReplicationThrottledForTier(TIER_1));
      throttler.incrementAssignedReplicas(TIER_1);

      Assertions.assertFalse(throttler.isReplicationThrottledForTier(TIER_2));
      throttler.incrementAssignedReplicas(TIER_2);
    }
  }

  @Test
  public void testTierWithLoadingReplicasDoesNotViolateThrottleLimit()
  {
    final int replicationThrottleLimit = 10;
    ReplicationThrottler throttler = new ReplicationThrottler(
        ImmutableMap.of(TIER_1, 10, TIER_2, 7),
        replicationThrottleLimit
    );

    // T1 cannot be assigned any more replicas
    Assertions.assertTrue(throttler.isReplicationThrottledForTier(TIER_1));

    // T2 can be assigned replicas until it hits the limit
    for (int i = 0; i < 3; ++i) {
      Assertions.assertFalse(throttler.isReplicationThrottledForTier(TIER_2));
      throttler.incrementAssignedReplicas(TIER_2);
    }
    Assertions.assertTrue(throttler.isReplicationThrottledForTier(TIER_2));
  }

}
