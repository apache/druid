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
 * Counts of replicas of a segment in different states.
 */
public class SegmentReplicaCount
{
  private static final SegmentReplicaCount EMPTY_INSTANCE = new SegmentReplicaCount();

  private int possible;
  private int required;

  private int loaded;
  private int loadedBroadcast;

  private int loading;
  private int dropping;
  private int moving;

  static SegmentReplicaCount empty()
  {
    return EMPTY_INSTANCE;
  }

  void addLoaded()
  {
    ++loaded;
  }

  /**
   * Increments number of segments loaded on non-historical servers. This value
   * is used only for computing level of under-replication of broadcast segments.
   */
  void addLoadedBroadcast()
  {
    ++loadedBroadcast;
  }

  void addQueued(SegmentAction action)
  {
    switch (action) {
      case REPLICATE:
      case LOAD:
        ++loading;
        break;
      case MOVE_TO:
        ++moving;
        break;
      case DROP:
        ++dropping;
        break;
      default:
        break;
    }
  }

  void setRequired(int required)
  {
    this.required = required;
  }

  void setPossible(int possible)
  {
    this.possible = possible;
  }

  int loading()
  {
    return loading;
  }

  int moving()
  {
    return moving;
  }

  /**
   * Number of loaded replicas. This also includes replicas that are currently
   * being dropped.
   */
  public int loaded()
  {
    return loaded;
  }

  /**
   * Number of replicas which are safely loaded and are not being dropped.
   */
  int loadedNotDropping()
  {
    return loaded - dropping;
  }

  int underReplicated(boolean ignoreMissingServers)
  {
    int totalLoaded = loaded + loadedBroadcast;
    int targetCount = ignoreMissingServers ? required : Math.min(required, possible);
    return targetCount > totalLoaded ? targetCount - totalLoaded : 0;
  }

  /**
   * Accumulates counts from the given {@code SegmentReplicaCount} into this instance.
   */
  void accumulate(SegmentReplicaCount other)
  {
    this.possible += other.possible;
    this.required += other.required;

    this.loaded += other.loaded;
    this.loadedBroadcast += other.loadedBroadcast;

    this.loading += other.loading;
    this.dropping += other.dropping;
    this.moving += other.moving;
  }
}
