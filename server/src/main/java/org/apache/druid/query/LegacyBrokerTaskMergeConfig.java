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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @deprecated These config paths were removed in Druid 33 and this class has
 * been retained only to generate appropriate error messages.
 */
public class LegacyBrokerTaskMergeConfig
{
  @JsonProperty
  private final Integer mergePoolTargetTaskRunTimeMillis;
  @JsonProperty
  private final Integer mergePoolTaskInitialYieldRows;
  @JsonProperty
  private final Integer mergePoolSmallBatchRows;

  @JsonCreator
  public LegacyBrokerTaskMergeConfig(
      @JsonProperty("smallBatchNumRows") Integer mergePoolSmallBatchRows,
      @JsonProperty("initialYieldNumRows") Integer mergePoolTaskInitialYieldRows,
      @JsonProperty("targetRunTimeMillis") Integer mergePoolTargetTaskRunTimeMillis
  )
  {
    this.mergePoolSmallBatchRows = mergePoolSmallBatchRows;
    this.mergePoolTaskInitialYieldRows = mergePoolTaskInitialYieldRows;
    this.mergePoolTargetTaskRunTimeMillis = mergePoolTargetTaskRunTimeMillis;
  }

  public Integer getMergePoolTargetTaskRunTimeMillis()
  {
    return mergePoolTargetTaskRunTimeMillis;
  }

  public Integer getMergePoolTaskInitialYieldRows()
  {
    return mergePoolTaskInitialYieldRows;
  }

  public Integer getMergePoolSmallBatchRows()
  {
    return mergePoolSmallBatchRows;
  }
}
