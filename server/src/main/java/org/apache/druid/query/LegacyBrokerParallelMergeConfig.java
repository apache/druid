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

import org.skife.config.Config;

import javax.annotation.Nullable;

/**
 * Backwards compatibility for Druid 27 and older runtime.properties configs, replaced by
 * {@link BrokerParallelMergeConfig} in Druid 28. This config should be removed in Druid 29.
 */
@Deprecated
public abstract class LegacyBrokerParallelMergeConfig
{
  @Nullable
  @Config(value = "druid.processing.merge.pool.parallelism")
  public Integer getMergePoolParallelism()
  {
    return null;
  }

  @Nullable
  @Config(value = "druid.processing.merge.pool.awaitShutdownMillis")
  public Long getMergePoolAwaitShutdownMillis()
  {
    return null;
  }

  @Nullable
  @Config(value = "druid.processing.merge.pool.defaultMaxQueryParallelism")
  public Integer getMergePoolDefaultMaxQueryParallelism()
  {
    return null;
  }

  @Nullable
  @Config(value = "druid.processing.merge.task.targetRunTimeMillis")
  public Integer getMergePoolTargetTaskRunTimeMillis()
  {
    return null;
  }

  @Nullable
  @Config(value = "druid.processing.merge.task.initialYieldNumRows")
  public Integer getMergePoolTaskInitialYieldRows()
  {
    return null;
  }

  @Nullable
  @Config(value = "druid.processing.merge.task.smallBatchNumRows")
  public Integer getMergePoolSmallBatchRows()
  {
    return null;
  }
}
