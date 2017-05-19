/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

/**
 * This enum represents types of druid services that hold segments.
 *
 * These types are externally visible (e.g., from the output of /druid/coordinator/v1/servers).
 *
 * For backwards compatibility, when presenting these types externally, the toString() representation
 * of the enum should be used.
 *
 * The toString() method converts the enum name() to lowercase and replaces underscores with hyphens,
 * which is the format expected for the server type string prior to the patch that introduced ServerType:
 * https://github.com/druid-io/druid/pull/4148
 */
public enum ServerType
{
  HISTORICAL,
  BRIDGE,
  INDEXER_EXECUTOR {
    @Override
    public boolean isSegmentReplicationTarget()
    {
      return false;
    }
  },

  REALTIME {
    @Override
    public boolean isSegmentReplicationTarget()
    {
      return false;
    }
  };

  /**
   * Indicates this type of node is able to be a target of segment replication.

   * @return true if it is available for replication
   *
   * @see io.druid.server.coordinator.rules.LoadRule
   */
  boolean isSegmentReplicationTarget()
  {
    return true;
  }

  /**
   * Indicates this type of node is able to be a target of segment broadcast.
   *
   * @return true if it is available for broadcast.
   */
  boolean isSegmentBroadcastTarget()
  {
    return true;
  }

  static ServerType fromString(String type)
  {
    return ServerType.valueOf(type.toUpperCase().replace("-", "_"));
  }

  @Override
  public String toString()
  {
    return name().toLowerCase().replace("_", "-");
  }
}
