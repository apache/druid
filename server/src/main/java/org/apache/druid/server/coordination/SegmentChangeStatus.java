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
 * Contains {@link State} of a {@link DataSegmentChangeRequest} and failure
 * message, if any.
 */
public class SegmentChangeStatus
{
  public enum State
  {
    SUCCESS, FAILED, PENDING
  }

  private final State state;
  @Nullable
  private final String failureCause;

  public static final SegmentChangeStatus SUCCESS = new SegmentChangeStatus(State.SUCCESS, null);
  public static final SegmentChangeStatus PENDING = new SegmentChangeStatus(State.PENDING, null);

  public static SegmentChangeStatus failed(String cause)
  {
    return new SegmentChangeStatus(State.FAILED, cause);
  }

  @JsonCreator
  private SegmentChangeStatus(
      @JsonProperty("state") State state,
      @JsonProperty("failureCause") @Nullable String failureCause
  )
  {
    this.state = Preconditions.checkNotNull(state, "state must be non-null");
    this.failureCause = failureCause;
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
    return "SegmentChangeStatus{" +
           "state=" + state +
           ", failureCause='" + failureCause + '\'' +
           '}';
  }
}
