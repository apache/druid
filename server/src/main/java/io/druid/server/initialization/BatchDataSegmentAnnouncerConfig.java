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

package io.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 */
public class BatchDataSegmentAnnouncerConfig
{
  @JsonProperty
  @Min(1)
  private int segmentsPerNode = 50;

  @JsonProperty
  @Max(1024 * 1024)
  @Min(1024)
  private long maxBytesPerNode = 512 * 1024;

  // Skip LoadSpec from segment announcements
  @JsonProperty
  private boolean skipLoadSpec = false;

  // Skip dimension list from segment announcements
  @JsonProperty
  private boolean skipDimensionsAndMetrics = false;

  @JsonProperty
  private boolean skipSegmentAnnouncementOnZk = false;

  public int getSegmentsPerNode()
  {
    return segmentsPerNode;
  }

  public long getMaxBytesPerNode()
  {
    return maxBytesPerNode;
  }

  public boolean isSkipLoadSpec()
  {
    return skipLoadSpec;
  }

  public boolean isSkipDimensionsAndMetrics()
  {
    return skipDimensionsAndMetrics;
  }

  public boolean isSkipSegmentAnnouncementOnZk()
  {
    return skipSegmentAnnouncementOnZk;
  }
}
