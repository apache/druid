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

package org.apache.druid.server.metrics;

import java.util.Collection;

/**
 * Provides supervisor stats for metrics emission. Used by {@link SupervisorStatsMonitor}
 * to report supervisor count and state metrics.
 */
public interface SupervisorStatsProvider
{
  /**
   * Returns a snapshot of per-supervisor stats for the current emission period.
   * Each entry represents one active supervisor with its id, type, and state.
   *
   * @return collection of supervisor stats; empty if no supervisors are active
   */
  Collection<SupervisorStats> getSupervisorStats();

  /**
   * Immutable snapshot of a single supervisor's stats for metrics emission.
   */
  class SupervisorStats
  {
    private final String supervisorId;
    private final String type;
    private final String state;
    private final String dataSource;
    private final String stream;
    private final String detailedState;

    public SupervisorStats(String supervisorId, String type, String state, String dataSource, String stream, String detailedState)
    {
      this.supervisorId = supervisorId;
      this.type = type;
      this.state = state;
      this.dataSource = dataSource;
      this.stream = stream;
      this.detailedState = detailedState;
    }

    public String getSupervisorId()
    {
      return supervisorId;
    }

    public String getType()
    {
      return type;
    }

    public String getState()
    {
      return state;
    }

    public String getDataSource()
    {
      return dataSource;
    }

    public String getStream()
    {
      return stream;
    }

    public String getDetailedState()
    {
      return detailedState;
    }
  }
}
