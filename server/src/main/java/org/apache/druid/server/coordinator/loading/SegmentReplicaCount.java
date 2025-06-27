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
 * Counts the number of replicas of a segment in different states (loading, loaded, etc)
 * in a tier or the whole cluster.
 */
public class SegmentReplicaCount
{
  private int requiredAndLoadable;
  private int required;

  private int loaded;
  private int loadedNonHistorical;

  private int loading;
  private int dropping;
  private int movingTo;
  private int movingFrom;

  /**
   * Increments number of replicas loaded on historical servers.
   */
  void incrementLoaded()
  {
    ++loaded;
  }

  /**
   * Increments number of replicas loaded on non-historical servers. This value
   * is used only for computing level of under-replication of broadcast segments.
   */
  void incrementLoadedOnNonHistoricalServer()
  {
    ++loadedNonHistorical;
  }

  /**
   * Increments number of replicas queued for the given action.
   */
  void incrementQueued(SegmentAction action)
  {
    switch (action) {
      case REPLICATE:
      case LOAD:
        ++loading;
        break;
      case MOVE_TO:
        ++movingTo;
        break;
      case MOVE_FROM:
        ++movingFrom;
        break;
      case DROP:
        ++dropping;
        break;
      default:
        break;
    }
  }

  /**
   * Sets the number of required replicas of this segment.
   *
   * @param required          Number of replicas as required by load or broadcast rules.
   * @param numLoadingServers Number of servers that can load replicas of this segment.
   */
  void setRequired(int required, int numLoadingServers)
  {
    this.required = required;
    this.requiredAndLoadable = Math.min(required, numLoadingServers);
  }

  /**
   * Required number of replicas of the segment as dictated by load rules.
   * This includes replicas that may be in excess of the cluster capacity.
   */
  public int required()
  {
    return required;
  }

  /**
   * Required number of replicas of the segment as dictated by load rules.
   * This does not include replicas that are in excess of the cluster capacity.
   */
  public int requiredAndLoadable()
  {
    return requiredAndLoadable;
  }

  int loading()
  {
    return loading;
  }

  int moving()
  {
    return movingTo;
  }

  /**
   * Number of moving segments which have been loaded on the target server but
   * are yet to be dropped from the source server. This value can be negative
   * only if the source server disappears before the move has finished.
   */
  int moveCompletedPendingDrop()
  {
    return movingFrom - movingTo;
  }

  /**
   * Number of replicas loaded on all servers. This includes replicas that are
   * currently being dropped.
   */
  public int totalLoaded()
  {
    return loaded + loadedNonHistorical;
  }

  /**
   * Number of replicas which are safely loaded on historical servers and are
   * not being dropped.
   */
  int loadedNotDropping()
  {
    return loaded - dropping;
  }

  /**
   * Number of replicas that are required to be loaded but are missing.
   * This includes replicas that may be in excess of the cluster capacity.
   */
  int missing()
  {
    return Math.max(required() - totalLoaded(), 0);
  }

  /**
   * Number of replicas that are required to be loaded but are missing.
   * This does not include replicas that are in excess of the cluster capacity.
   */
  int missingAndLoadable()
  {
    return Math.max(requiredAndLoadable() - totalLoaded(), 0);
  }

  /**
   * Accumulates counts from the given {@code SegmentReplicaCount} into this instance.
   */
  void accumulate(SegmentReplicaCount other)
  {
    this.required += other.required;
    this.requiredAndLoadable += other.requiredAndLoadable;

    this.loaded += other.loaded;
    this.loadedNonHistorical += other.loadedNonHistorical;

    this.loading += other.loading;
    this.dropping += other.dropping;
    this.movingTo += other.movingTo;
    this.movingFrom += other.movingFrom;
  }
}
