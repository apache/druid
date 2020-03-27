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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamVerifierSyntheticEvent
{
  private String id;
  private long groupingTimestamp;
  private long insertionTimestamp;
  private long sequenceNumber;
  private Long expectedSequenceNumberSum;
  private boolean firstEvent;

  public StreamVerifierSyntheticEvent(
      String id,
      long groupingTimestamp,
      long insertionTimestamp,
      long sequenceNumber,
      Long expectedSequenceNumberSum,
      boolean firstEvent
  )
  {
    this.id = id;
    this.groupingTimestamp = groupingTimestamp;
    this.insertionTimestamp = insertionTimestamp;
    this.sequenceNumber = sequenceNumber;
    this.expectedSequenceNumberSum = expectedSequenceNumberSum;
    this.firstEvent = firstEvent;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public long getGroupingTimestamp()
  {
    return groupingTimestamp;
  }

  @JsonProperty
  public long getInsertionTimestamp()
  {
    return insertionTimestamp;
  }

  @JsonProperty
  public long getSequenceNumber()
  {
    return sequenceNumber;
  }

  @JsonProperty
  public Long getExpectedSequenceNumberSum()
  {
    return expectedSequenceNumberSum;
  }

  @JsonProperty
  public Integer getFirstEventFlag()
  {
    return firstEvent ? 1 : null;
  }

  public static StreamVerifierSyntheticEvent of(
      String id,
      long groupingTimestamp,
      long insertionTimestamp,
      long sequenceNumber,
      Long expectedSequenceNumberSum,
      boolean firstEvent
  )
  {
    return new StreamVerifierSyntheticEvent(
        id,
        groupingTimestamp,
        insertionTimestamp,
        sequenceNumber,
        expectedSequenceNumberSum,
        firstEvent
    );
  }
}
