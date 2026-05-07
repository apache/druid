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

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Map key used by {@link SegmentReplicaCountMap}. A null {@code version} represents tier-wide
 * tracking; a non-null {@code version} represents a specific version within the tier
 * (used when the coordinator is enforcing per-version replication via {@code coordinatingVersions}).
 */
public record ReplicaCountKey(String tier, @Nullable String version)
{
  public static ReplicaCountKey forTier(String tier)
  {
    return new ReplicaCountKey(tier, null);
  }

  /**
   * Returns a (tier, version) key when {@code version} is non-null and present in
   * {@code coordinatingVersions}; otherwise a plain tier-wide key.
   */
  public static ReplicaCountKey from(String tier, @Nullable String version, Set<String> coordinatingVersions)
  {
    if (version != null && coordinatingVersions.contains(version)) {
      return new ReplicaCountKey(tier, version);
    }
    return new ReplicaCountKey(tier, null);
  }
}
