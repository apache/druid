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

import java.util.List;

/**
 * Return type of {@link ChangeRequestHistory#getRequestsSince}.
 */
public final class ChangeRequestsSnapshot<T>
{

  public static <T> ChangeRequestsSnapshot<T> success(ChangeRequestHistory.Counter counter, List<T> requests)
  {
    return new ChangeRequestsSnapshot<>(false, null, counter, requests);
  }

  public static <T> ChangeRequestsSnapshot<T> fail(String resetCause)
  {
    return new ChangeRequestsSnapshot<>(true, resetCause, null, null);
  }

  /** if true, that means caller should reset the counter and request again. */
  private final boolean resetCounter;

  /** cause for reset if {@link #resetCounter} is true */
  @Nullable
  private final String resetCause;

  /** segments requests delta since counter, if {@link #resetCounter} if false */
  @Nullable
  private final ChangeRequestHistory.Counter counter;
  @Nullable
  private final List<T> requests;

  @JsonCreator
  public ChangeRequestsSnapshot(
      @JsonProperty("resetCounter") boolean resetCounter,
      @JsonProperty("resetCause") @Nullable String resetCause,
      @JsonProperty("counter") @Nullable ChangeRequestHistory.Counter counter,
      @JsonProperty("requests") @Nullable List<T> requests
  )
  {
    this.resetCounter = resetCounter;
    this.resetCause = resetCause;

    if (resetCounter) {
      Preconditions.checkNotNull(resetCause, "NULL resetCause when resetCounter is true.");
    }

    this.counter = counter;
    this.requests = requests;
  }

  @JsonProperty
  public boolean isResetCounter()
  {
    return resetCounter;
  }

  @Nullable
  @JsonProperty
  public String getResetCause()
  {
    return resetCause;
  }

  @Nullable
  @JsonProperty
  public ChangeRequestHistory.Counter getCounter()
  {
    return counter;
  }

  @Nullable
  @JsonProperty
  public List<T> getRequests()
  {
    return requests;
  }

  @Override
  public String toString()
  {
    return "ChangeRequestsSnapshot{" +
           "resetCounter=" + resetCounter +
           ", resetCause='" + resetCause + '\'' +
           ", counter=" + counter +
           ", requests=" + requests +
           '}';
  }
}
