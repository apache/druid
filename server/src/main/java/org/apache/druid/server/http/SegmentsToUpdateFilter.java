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

package org.apache.druid.server.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Filter to identify segments that need to be updated via REST APIs.
 */
public class SegmentsToUpdateFilter
{
  private final Interval interval;
  private final Set<String> segmentIds;
  private final List<String> versions;

  public static final String INVALID_PAYLOAD_ERROR_MESSAGE =
      "Invalid request payload. Specify either 'interval' or 'segmentIds', but not both."
      + " Optionally, include 'versions' only when 'interval' is provided.";

  @JsonCreator
  public SegmentsToUpdateFilter(
      @JsonProperty("interval") @Nullable Interval interval,
      @JsonProperty("segmentIds") @Nullable Set<String> segmentIds,
      @JsonProperty("versions") @Nullable List<String> versions
  )
  {
    this.interval = interval;
    this.segmentIds = segmentIds;
    this.versions = versions;
  }

  @Nullable
  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  @JsonProperty
  public Set<String> getSegmentIds()
  {
    return segmentIds;
  }

  @Nullable
  @JsonProperty
  public List<String> getVersions()
  {
    return versions;
  }

  /**
   * The filter is valid if either {@code interval} or {@code segmentIds} is specified, but not both.
   * {@code versions} may be optionally specified only when {@code interval} is provided.
   */
  public boolean isValid()
  {
    final boolean hasSegmentIds = !CollectionUtils.isNullOrEmpty(segmentIds);
    if (interval == null) {
      return hasSegmentIds && CollectionUtils.isNullOrEmpty(versions);
    } else {
      return !hasSegmentIds;
    }
  }
}
