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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.server.coordination.ServerType;

import java.util.Set;

/**
 * Decides the types of data servers contacted by MSQ tasks to fetch results.
 */
public enum SegmentSource
{
  /**
   * Include only segments from deep storage.
   */
  NONE(ImmutableSet.of()),
  /**
   * Include segments from realtime tasks as well as segments from deep storage.
   */
  REALTIME(ImmutableSet.of(ServerType.REALTIME, ServerType.INDEXER_EXECUTOR));

  /**
   * The type of dataservers (if any) to include. This does not include segments queried from deep storage, which are
   * always included in queries.
   */
  private final Set<ServerType> usedServerTypes;

  SegmentSource(Set<ServerType> usedServerTypes)
  {
    this.usedServerTypes = usedServerTypes;
  }

  public Set<ServerType> getUsedServerTypes()
  {
    return usedServerTypes;
  }

  /**
   * Whether realtime servers should be included for the segmentSource.
   */
  public static boolean shouldQueryRealtimeServers(SegmentSource segmentSource)
  {
    return REALTIME.equals(segmentSource);
  }
}
