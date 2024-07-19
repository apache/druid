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

/**
 * Response of a {@link DataSegmentChangeRequest}. Contains the request itself
 * and the result {@link SegmentChangeStatus}.
 */
public class DataSegmentChangeResponse
{
  private final DataSegmentChangeRequest request;
  private final SegmentChangeStatus status;

  @JsonCreator
  public DataSegmentChangeResponse(
      @JsonProperty("request") DataSegmentChangeRequest request,
      @JsonProperty("status") SegmentChangeStatus status
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
  public SegmentChangeStatus getStatus()
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
}
