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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 * Class sent by worker to controller after reading input to generate partition boundries.
 */
public class ClusterByStatisticsWorkerReport
{
  private final Set<Integer> workerIds; // TODO: change to be a map of rowkey vs worker id. Currently this isn't very useful.

  private Boolean hasMultipleValues;

  @JsonCreator
  public ClusterByStatisticsWorkerReport(
      @JsonProperty("workerIds") final Set<Integer> timeChunks,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues
  )
  {
    this.workerIds = timeChunks;
    this.hasMultipleValues = hasMultipleValues;
  }

  public void addAll(ClusterByStatisticsWorkerReport other)
  {
    workerIds.addAll(other.getWorkerIds());
    hasMultipleValues = hasMultipleValues || other.hasMultipleValues;
  }

  @JsonProperty("workerIds")
  public Set<Integer> getWorkerIds()
  {
    return workerIds;
  }

  @JsonProperty("hasMultipleValues")
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }
}
