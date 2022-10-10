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

/**
 * Contains constants related to coordinator metrics.
 */
public class Metrics
{
  private Metrics()
  {
    // no instantiation
  }

  public static final String CANCELLED_MOVES = "cancelMoveCount";
  public static final String CANCELLED_LOADS = "cancelLoadCount";
  public static final String CANCELLED_DROPS = "cancelDropCount";
  public static final String QUEUED_LOADS = "assignedCount";
  public static final String QUEUED_DROPS = "droppedCount";
  public static final String BROADCAST_LOADS = "broadcastLoadCount";
  public static final String BROADCAST_DROPS = "broadcastDropCount";
  public static final String DELETED_SEGMENTS = "deletedCount";

  public static final String REQUIRED_CAPACITY = "requiredCapacity";

  public static class Segment
  {
    public static final String SIZE = "segment/size";
    public static final String COUNT = "segment/count";

    public static final String ASSIGNED_COUNT = "segment/assigned/count";
    public static final String DROPPED_COUNT = "segment/dropped/count";
    public static final String MOVED_COUNT = "segment/moved/count";
    public static final String DELETED_COUNT = "segment/deleted/count";

    public static final String UNNEEDED_COUNT = "segment/unneeded/count";
    public static final String OVERSHADOWED_COUNT = "segment/overShadowed/count";
    public static final String UNDER_REPLICATED_COUNT = "segment/underReplicated/count";
    public static final String UNAVAILABLE_COUNT = "segment/unavailable/count";
  }

  public static class LoadQueue
  {
    public static final String SIZE = "segment/loadQueue/size";
    public static final String COUNT = "segment/loadQueue/count";
    public static final String FAILED = "segment/loadQueue/failed";

  }

  public static class DropQueue
  {
    public static final String COUNT = "segment/loadQueue/count";
  }

  public static class Coordinator
  {

  }

}
