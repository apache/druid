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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Response of a {@link DataSegmentChangeRequest}. Contains the request itself
 * and the {@link Status} of the request.
 */
public class DataSegmentChangeResponse
{
  private final DataSegmentChangeRequest request;
  private final Status status;

  @JsonCreator
  public DataSegmentChangeResponse(
      @JsonProperty("request") DataSegmentChangeRequest request,
      @JsonProperty("status") Status status
  )
  {
    this.request = request;
    this.status = status;
  }

  @JsonProperty
  public DataSegmentChangeRequest getRequest()
  {
    return request;
  }

  @JsonProperty
  public Status getStatus()
  {
    return status;
  }

  @Override
  public String toString()
  {
    return "DataSegmentChangeResponse{" +
           "request=" + request +
           ", status=" + status +
           '}';
  }

  public enum State
  {
    SUCCESS, FAILED, PENDING
  }

  /**
   * Contains {@link State} of a {@link DataSegmentChangeRequest} and the failure
   * message, if any.
   */
  public static class Status
  {
    private final State state;
    @Nullable
    private final String failureCause;

    public static final Status SUCCESS = new Status(State.SUCCESS, null);
    public static final Status PENDING = new Status(State.PENDING, null);

    @JsonCreator
    public Status(
        @JsonProperty("state") State state,
        @JsonProperty("failureCause") @Nullable String failureCause
    )
    {
      Preconditions.checkNotNull(state, "state must be non-null");
      this.state = state;
      this.failureCause = failureCause;
    }

    public static Status failed(String cause)
    {
      return new Status(State.FAILED, cause);
    }

    @JsonProperty
    public State getState()
    {
      return state;
    }

    @Nullable
    @JsonProperty
    public String getFailureCause()
    {
      return failureCause;
    }

    @Override
    public String toString()
    {
      return "Status{" +
             "state=" + state +
             ", failureCause='" + failureCause + '\'' +
             '}';
    }
  }
}
