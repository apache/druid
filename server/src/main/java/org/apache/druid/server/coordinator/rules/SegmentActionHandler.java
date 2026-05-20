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

import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

/**
 * Performs various actions on a given segment. Used by {@link Rule}s to load,
 * drop, broadcast or delete segments.
 */
public interface SegmentActionHandler
{

  /**
   * Queues load or drop of replicas of the given segment to achieve the
   * target replication level on all historical tiers.
   */
  void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount);

  /**
   * Like {@link #replicateSegment(DataSegment, Map)} but for a partial-load rule. The given {@code profile} carries
   * the partial load spec and rule fingerprint that the historicals are being asked to load and that the
   * coordinator uses to reconcile already-loaded replicas against the rule's request.
   * <p>
   * Default implementation throws {@link UnsupportedOperationException}: callers that don't intend to support partial
   * load rules (e.g., minimal mock handlers used in unit tests) can leave it unimplemented. The production handler
   * {@code StrategicSegmentAssigner} overrides this to do fingerprint-aware replica counting.
   */
  default void replicateSegmentPartially(
      DataSegment segment,
      PartialLoadProfile profile,
      Map<String, Integer> tierToReplicaCount
  )
  {
    throw new UnsupportedOperationException(
        "replicateSegmentPartially is not supported by this SegmentActionHandler implementation"
    );
  }

  /**
   * Marks the given segment as unused. Unused segments are eventually unloaded
   * from all servers and deleted from metadata as well as deep storage.
   */
  void deleteSegment(DataSegment segment);

  /**
   * Broadcasts the given segment to all servers that are broadcast targets.
   */
  void broadcastSegment(DataSegment segment);

}
