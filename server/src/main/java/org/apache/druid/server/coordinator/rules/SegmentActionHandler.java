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
   * Marks the given segment as unused. Unused segments are eventually unloaded
   * from all servers and deleted from metadata as well as deep storage.
   */
  void deleteSegment(DataSegment segment);

  /**
   * Broadcasts the given segment to all servers that are broadcast targets.
   */
  void broadcastSegment(DataSegment segment);

}
