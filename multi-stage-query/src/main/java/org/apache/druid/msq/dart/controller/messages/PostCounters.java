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

package org.apache.druid.msq.dart.controller.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;

import java.util.Objects;

/**
 * Message for {@link ControllerClient#postCounters}.
 */
public class PostCounters implements ControllerMessage
{
  private final String queryId;
  private final String workerId;
  private final CounterSnapshotsTree counters;

  @JsonCreator
  public PostCounters(
      @JsonProperty("queryId") final String queryId,
      @JsonProperty("workerId") final String workerId,
      @JsonProperty("counters") final CounterSnapshotsTree counters
  )
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    this.workerId = Preconditions.checkNotNull(workerId, "workerId");
    this.counters = Preconditions.checkNotNull(counters, "counters");
  }

  @Override
  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }

  @JsonProperty
  public String getWorkerId()
  {
    return workerId;
  }

  @JsonProperty("counters")
  public CounterSnapshotsTree getCounters()
  {
    return counters;
  }

  @Override
  public void handle(final Controller controller)
  {
    controller.updateCounters(workerId, counters);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PostCounters that = (PostCounters) o;
    return Objects.equals(queryId, that.queryId)
           && Objects.equals(workerId, that.workerId)
           && Objects.equals(counters, that.counters);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryId, workerId, counters);
  }

  @Override
  public String toString()
  {
    return "PostCounters{" +
           "queryId='" + queryId + '\'' +
           ", workerId='" + workerId + '\'' +
           ", counters=" + counters +
           '}';
  }
}
