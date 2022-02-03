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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Should be synchronized with org.apache.druid.indexing.overlord.http.AutoScaleConfigResponse
 */
public class IndexingAutoScaleConfigInfo
{
  private final int minNumWorkers;
  private final int maxNumWorkers;
  private final int workerCapacityHint;

  @JsonCreator
  public IndexingAutoScaleConfigInfo(
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("workerCapacityHint") int workerCapacityHint
  )
  {
    this.minNumWorkers = minNumWorkers;
    this.maxNumWorkers = maxNumWorkers;
    this.workerCapacityHint = workerCapacityHint;
  }

  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
  }

  @JsonProperty
  public int getMaxNumWorkers()
  {
    return maxNumWorkers;
  }

  @JsonProperty
  public int getWorkerCapacityHint()
  {
    return workerCapacityHint;
  }
}
