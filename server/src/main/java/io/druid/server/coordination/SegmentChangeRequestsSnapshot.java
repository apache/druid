/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Return type of SegmentChangeRequestHistory.getRequestsSince(counter).
 */
public class SegmentChangeRequestsSnapshot
{
  //if true, that means caller should reset the counter and request again.
  private final boolean resetCounter;

  //cause for reset if resetCounter is true
  private final String resetCause;

  //segments requests delta since counter, if resetCounter if false
  private final SegmentChangeRequestHistory.Counter  counter;
  private final List<DataSegmentChangeRequest> requests;

  @JsonCreator
  public SegmentChangeRequestsSnapshot(
      @JsonProperty("resetCounter") boolean resetCounter,
      @JsonProperty("resetCause") String resetCause,
      @JsonProperty("counter") SegmentChangeRequestHistory.Counter counter,
      @JsonProperty("requests") List<DataSegmentChangeRequest> requests
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

  public static SegmentChangeRequestsSnapshot success(SegmentChangeRequestHistory.Counter counter,
                                                      List<DataSegmentChangeRequest> requests)
  {
    return new SegmentChangeRequestsSnapshot(false, null, counter, requests);
  }

  public static SegmentChangeRequestsSnapshot fail(String resetCause)
  {
    return new SegmentChangeRequestsSnapshot(true, resetCause, null, null);
  }

  @JsonProperty
  public boolean isResetCounter()
  {
    return resetCounter;
  }

  @JsonProperty
  public String getResetCause()
  {
    return resetCause;
  }

  @JsonProperty
  public SegmentChangeRequestHistory.Counter getCounter()
  {
    return counter;
  }

  @JsonProperty
  public List<DataSegmentChangeRequest> getRequests()
  {
    return requests;
  }

  @Override
  public String toString()
  {
    return "SegmentChangeRequestsSnapshot{" +
           "resetCounter=" + resetCounter +
           ", resetCause='" + resetCause + '\'' +
           ", counter=" + counter +
           ", requests=" + requests +
           '}';
  }
}
