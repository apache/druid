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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Should be synchronized with org.apache.druid.indexing.common.TaskStatus.
 */
public class QueryStatus
{
  public enum Status
  {
    RUNNING,
    SUCCESS,
    FAILED
  }

  private final String id;
  private final Status status;
  private final long duration;

  @JsonCreator
  public QueryStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") Status status,
      @JsonProperty("duration") long duration
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.status = Preconditions.checkNotNull(status, "status");
    this.duration = duration;
  }

  @JsonProperty("id")
  public String getId()
  {
    return id;
  }

  @JsonProperty("status")
  public Status getStatusCode()
  {
    return status;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  @JsonIgnore
  public boolean isComplete()
  {
    return status != Status.RUNNING;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("duration", duration)
                  .toString();
  }
}
