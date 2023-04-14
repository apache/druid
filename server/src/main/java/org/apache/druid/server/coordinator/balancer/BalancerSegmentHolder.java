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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

/**
 * Represents a segment picked for moving by a balancer strategy.
 */
public class BalancerSegmentHolder
{
  private final ServerHolder server;
  private final DataSegment segment;

  public BalancerSegmentHolder(ServerHolder server, DataSegment segment)
  {
    this.server = server;
    this.segment = segment;
  }

  public ServerHolder getServer()
  {
    return server;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

}
