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

package org.apache.druid.emitter.graphite;

import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;

public class GraphiteEvent
{
  private final String eventPath;
  private final String value;
  private final long timestamp;

  /**
   * A graphite event must be in the following format: <metric path> <metric value> <metric timestamp>
   *   ex:  PRODUCTION.host.graphite-tutorial.responseTime.p95 0.10 1400509112
   * @param eventPath This is the namespace path of the metric
   * @param value value of the metric
   * @param timestamp unix time in second
   */
  GraphiteEvent(@NotNull String eventPath, @NotNull String value, @NotNull Long timestamp)
  {
    this.eventPath = Preconditions.checkNotNull(eventPath, "path can not be null");
    this.value = Preconditions.checkNotNull(value, "value can not be null");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp can not be null");
  }

  public String getEventPath()
  {
    return eventPath;
  }

  public String getValue()
  {
    return value;
  }

  public long getTimestamp()
  {
    return timestamp;
  }
}
