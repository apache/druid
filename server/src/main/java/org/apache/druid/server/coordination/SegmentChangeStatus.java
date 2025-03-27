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
import org.apache.druid.server.http.SegmentLoadingMode;

import javax.annotation.Nullable;
import java.util.Objects;

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
  private final SegmentLoadingMode loadingMode;

  private static final SegmentChangeStatus SUCCESS = new SegmentChangeStatus(State.SUCCESS, null, null);
  private static final SegmentChangeStatus PENDING = new SegmentChangeStatus(State.PENDING, null, null);

  public static SegmentChangeStatus success()
  {
    return SUCCESS;
  }

  public static SegmentChangeStatus success(SegmentLoadingMode loadingMode)
  {
    return new SegmentChangeStatus(State.SUCCESS, null, loadingMode);
  }

  public static SegmentChangeStatus pending()
  {
    return PENDING;
  }

  public static SegmentChangeStatus pending(SegmentLoadingMode loadingMode)
  {
    return new SegmentChangeStatus(State.PENDING, null, loadingMode);
  }

  public static SegmentChangeStatus failed(String cause, SegmentLoadingMode loadingMode)
  {
    return new SegmentChangeStatus(State.FAILED, cause, loadingMode);
  }

  public static SegmentChangeStatus failed(String cause)
  {
    return new SegmentChangeStatus(State.FAILED, cause, null);
  }

  @JsonCreator
  private SegmentChangeStatus(
      @JsonProperty("state") State state,
      @JsonProperty("failureCause") @Nullable String failureCause,
      @JsonProperty("loadingMode") @Nullable SegmentLoadingMode loadingMode
  )
  {
    this.state = Preconditions.checkNotNull(state, "state must be non-null");
    this.failureCause = failureCause;
    this.loadingMode = loadingMode;
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

  @Nullable
  @JsonProperty
  public SegmentLoadingMode getLoadingMode()
  {
    return loadingMode;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentChangeStatus that = (SegmentChangeStatus) o;
    return state == that.state
           && Objects.equals(failureCause, that.failureCause)
           && loadingMode == that.loadingMode;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(state, failureCause, loadingMode);
  }

  @Override
  public String toString()
  {
    return "SegmentChangeStatus{" +
           "state=" + state +
           ", failureCause='" + failureCause + '\'' +
           ", loadingMode=" + loadingMode +
           '}';
  }
}
