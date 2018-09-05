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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.Objects;

public class RequestLogLineBase
{
  protected final DateTime timestamp;
  protected final String remoteAddr;
  protected final QueryStats queryStats;

  public RequestLogLineBase(DateTime timestamp, String remoteAddr, QueryStats queryStats)
  {
    this.timestamp = timestamp;
    this.remoteAddr = remoteAddr;
    this.queryStats = queryStats;
  }

  @JsonProperty("timestamp")
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty("remoteAddr")
  public String getRemoteAddr()
  {
    return remoteAddr;
  }

  @JsonProperty("queryStats")
  public QueryStats getQueryStats()
  {
    return queryStats;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RequestLogLineBase)) {
      return false;
    }
    RequestLogLineBase that = (RequestLogLineBase) o;
    return Objects.equals(timestamp, that.timestamp) &&
           Objects.equals(remoteAddr, that.remoteAddr) &&
           Objects.equals(queryStats, that.queryStats);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(timestamp, remoteAddr, queryStats);
  }
}

