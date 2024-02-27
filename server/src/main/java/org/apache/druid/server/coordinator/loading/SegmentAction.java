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

/**
 * Represents actions that can be performed on a server for a single segment.
 * <p>
 * The different action types can be used to prioritize items in a LoadQueuePeon.
 */
public enum SegmentAction
{
  /**
   * Drop a segment from a server.
   */
  DROP(false),

  /**
   * Load a segment on a server. This should be used when trying to load a segment
   * on a tier where it is currently unavailable (i.e. no replicas loaded).
   * This action cannot be throttled by the {@code replicationThrottleLimit}.
   */
  LOAD(true),

  /**
   * Load a replica of a segment on a server. This should be used when trying to
   * load more replicas of a segment on a tier where it is already available
   * (i.e. atleast one loaded replica).
   * <p>
   * This is different from LOAD in two ways:
   * <ul>
   *   <li>this action can be throttled by the {@code replicationThrottleLimit}</li>
   *   <li>it is given lower priority than LOAD on the load queue peon</li>
   * </ul>
   * For all other purposes, REPLICATE is treated the same as LOAD.
   */
  REPLICATE(true),

  /**
   * Move a segment to this server.
   */
  MOVE_TO(true),

  /**
   * Move a segment from this server to another. This is essentially a pending
   * DROP operation, which starts only when the corresponding MOVE_TO has succeded.
   */
  MOVE_FROM(false);

  private final boolean isLoad;

  SegmentAction(boolean isLoad)
  {
    this.isLoad = isLoad;
  }

  /**
   * True only if this action loads a segment on a server, i.e. LOAD, REPLICATE
   * or MOVE_TO.
   */
  public boolean isLoad()
  {
    return isLoad;
  }
}
