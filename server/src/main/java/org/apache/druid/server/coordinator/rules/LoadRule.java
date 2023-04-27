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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  @Override
  public void run(DataSegment segment, SegmentActionHandler handler)
  {
    handler.updateSegmentReplicasInTiers(segment, getTieredReplicants());
  }

  protected static void validateTieredReplicants(final Map<String, Integer> tieredReplicants)
  {
    if (tieredReplicants.size() == 0) {
      throw new IAE("A rule with empty tiered replicants is invalid");
    }
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      if (entry.getValue() == null) {
        throw new IAE("Replicant value cannot be empty");
      }
      if (entry.getValue() < 0) {
        throw new IAE("Replicant value [%d] is less than 0, which is not allowed", entry.getValue());
      }
    }
  }

  public abstract Map<String, Integer> getTieredReplicants();

  public abstract int getNumReplicants(String tier);

}
