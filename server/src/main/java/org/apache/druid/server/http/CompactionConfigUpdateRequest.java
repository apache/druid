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
import org.apache.druid.indexer.CompactionEngine;

import javax.annotation.Nullable;

/**
 * Payload to update the cluster-level compaction config.
 * All fields of this class must be nullable. A non-value indicates that the
 * corresponding field is being updated.
 */
public class CompactionConfigUpdateRequest
{
  private final Double compactionTaskSlotRatio;
  private final Integer maxCompactionTaskSlots;
  private final Boolean useAutoScaleSlots;
  private final CompactionEngine compactionEngine;

  @JsonCreator
  public CompactionConfigUpdateRequest(
      @JsonProperty("compactionTaskSlotRatio") @Nullable Double compactionTaskSlotRatio,
      @JsonProperty("maxCompactionTaskSlots") @Nullable Integer maxCompactionTaskSlots,
      @JsonProperty("useAutoScaleSlots") @Nullable Boolean useAutoScaleSlots,
      @JsonProperty("compactionEngine") @Nullable CompactionEngine compactionEngine
  )
  {
    this.compactionTaskSlotRatio = compactionTaskSlotRatio;
    this.maxCompactionTaskSlots = maxCompactionTaskSlots;
    this.useAutoScaleSlots = useAutoScaleSlots;
    this.compactionEngine = compactionEngine;
  }

  @Nullable
  @JsonProperty
  public Double getCompactionTaskSlotRatio()
  {
    return compactionTaskSlotRatio;
  }

  @Nullable
  @JsonProperty
  public Integer getMaxCompactionTaskSlots()
  {
    return maxCompactionTaskSlots;
  }

  @Nullable
  @JsonProperty
  public Boolean getUseAutoScaleSlots()
  {
    return useAutoScaleSlots;
  }

  @Nullable
  @JsonProperty
  public CompactionEngine getCompactionEngine()
  {
    return compactionEngine;
  }

}
