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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class AppenderatorDriverMetadata
{
  private final Map<String, List<SegmentIdentifier>> activeSegments;
  private final Map<String, List<SegmentIdentifier>> publishPendingSegments;
  private final Map<String, String> lastSegmentIds;
  private final Object callerMetadata;

  @JsonCreator
  public AppenderatorDriverMetadata(
      @JsonProperty("activeSegments") Map<String, List<SegmentIdentifier>> activeSegments,
      @JsonProperty("publishPendingSegments") Map<String, List<SegmentIdentifier>> publishPendingSegments,
      @JsonProperty("lastSegmentIds") Map<String, String> lastSegmentIds,
      @JsonProperty("callerMetadata") Object callerMetadata
  )
  {
    this.activeSegments = activeSegments;
    this.publishPendingSegments = publishPendingSegments;
    this.lastSegmentIds = lastSegmentIds;
    this.callerMetadata = callerMetadata;
  }

  @JsonProperty
  public Map<String, List<SegmentIdentifier>> getActiveSegments()
  {
    return activeSegments;
  }

  @JsonProperty
  public Map<String, List<SegmentIdentifier>> getPublishPendingSegments()
  {
    return publishPendingSegments;
  }

  @JsonProperty
  public Map<String, String> getLastSegmentIds()
  {
    return lastSegmentIds;
  }

  @JsonProperty
  public Object getCallerMetadata()
  {
    return callerMetadata;
  }

  @Override
  public String toString()
  {
    return "AppenderatorDriverMetadata{" +
           "activeSegments=" + activeSegments +
           ", publishPendingSegments=" + publishPendingSegments +
           ", lastSegmentIds=" + lastSegmentIds +
           ", callerMetadata=" + callerMetadata +
           '}';
  }
}
