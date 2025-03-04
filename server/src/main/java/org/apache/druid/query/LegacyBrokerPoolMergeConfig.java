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

import javax.annotation.Nullable;

/**
 * @deprecated These config paths were removed in Druid 33 and this class has
 * been retained only to generate appropriate error messages.
 */
@Deprecated
public class LegacyBrokerPoolMergeConfig
{
  @JsonProperty
  private final Integer mergePoolParallelism;
  @JsonProperty
  private final Long mergePoolAwaitShutdownMillis;
  @JsonProperty
  private final Integer mergePoolDefaultMaxQueryParallelism;

  @JsonCreator
  public LegacyBrokerPoolMergeConfig(
      @JsonProperty("parallelism") @Nullable Integer mergePoolParallelism,
      @JsonProperty("awaitShutdownMillis") @Nullable Long mergePoolAwaitShutdownMillis,
      @JsonProperty("defaultMaxQueryParallelism") @Nullable Integer mergePoolDefaultMaxQueryParallelism
  )
  {
    this.mergePoolParallelism = mergePoolParallelism;
    this.mergePoolAwaitShutdownMillis = mergePoolAwaitShutdownMillis;
    this.mergePoolDefaultMaxQueryParallelism = mergePoolDefaultMaxQueryParallelism;
  }

  public Integer getMergePoolDefaultMaxQueryParallelism()
  {
    return mergePoolDefaultMaxQueryParallelism;
  }

  public Integer getMergePoolParallelism()
  {
    return mergePoolParallelism;
  }

  public Long getMergePoolAwaitShutdownMillis()
  {
    return mergePoolAwaitShutdownMillis;
  }
}
