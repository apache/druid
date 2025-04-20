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
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;

public class BrokerParallelMergeConfig
{
  private static final Logger LOG = new Logger(BrokerParallelMergeConfig.class);
  public static final int DEFAULT_MERGE_POOL_AWAIT_SHUTDOWN_MILLIS = 60_000;

  @JsonProperty
  private final boolean useParallelMergePool;
  @JsonProperty
  private final int parallelism;
  @JsonProperty
  private final long awaitShutdownMillis;
  @JsonProperty
  private final int defaultMaxQueryParallelism;
  @JsonProperty
  private final int targetRunTimeMillis;
  @JsonProperty
  private final int initialYieldNumRows;
  @JsonProperty
  private final int smallBatchNumRows;

  @JsonCreator
  public BrokerParallelMergeConfig(
      @JsonProperty("useParallelMergePool") @Nullable Boolean useParallelMergePool,
      @JsonProperty("parallelism") @Nullable Integer parallelism,
      @JsonProperty("awaitShutdownMillis") @Nullable Long awaitShutdownMillis,
      @JsonProperty("defaultMaxQueryParallelism") @Nullable Integer defaultMaxQueryParallelism,
      @JsonProperty("targetRunTimeMillis") @Nullable Integer targetRunTimeMillis,
      @JsonProperty("initialYieldNumRows") @Nullable Integer initialYieldNumRows,
      @JsonProperty("smallBatchNumRows") @Nullable Integer smallBatchNumRows
  )
  {
    this.parallelism = Configs.valueOrDefault(
        parallelism,
        // assume 2 hyper-threads per core, so that this value is probably by default the number
        // of physical cores * 1.5
        (int) Math.ceil(JvmUtils.getRuntimeInfo().getAvailableProcessors() * 0.75)
    );

    // need at least 3 to do 2 layer merge
    if (this.parallelism > 2) {
      this.useParallelMergePool = useParallelMergePool == null || useParallelMergePool;
    } else {
      if (useParallelMergePool == null || useParallelMergePool) {
        LOG.debug(
            "Parallel merge pool is enabled, but there are not enough cores to enable parallel merges: %s",
            parallelism
        );
      }
      this.useParallelMergePool = false;
    }

    this.awaitShutdownMillis = Configs.valueOrDefault(
        awaitShutdownMillis,
        DEFAULT_MERGE_POOL_AWAIT_SHUTDOWN_MILLIS
    );

    this.defaultMaxQueryParallelism = Configs.valueOrDefault(
        defaultMaxQueryParallelism,
        (int) Math.max(JvmUtils.getRuntimeInfo().getAvailableProcessors() * 0.5, 1)
    );

    this.targetRunTimeMillis = Configs.valueOrDefault(
        targetRunTimeMillis,
        ParallelMergeCombiningSequence.DEFAULT_TASK_TARGET_RUN_TIME_MILLIS
    );

    this.initialYieldNumRows = Configs.valueOrDefault(
        initialYieldNumRows,
        ParallelMergeCombiningSequence.DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS
    );

    this.smallBatchNumRows = Configs.valueOrDefault(
        smallBatchNumRows,
        ParallelMergeCombiningSequence.DEFAULT_TASK_SMALL_BATCH_NUM_ROWS
    );
  }

  @VisibleForTesting
  public BrokerParallelMergeConfig()
  {
    this(null, null, null, null, null, null, null);
  }

  public boolean useParallelMergePool()
  {
    return useParallelMergePool;
  }

  public int getParallelism()
  {
    return parallelism;
  }

  public long getAwaitShutdownMillis()
  {
    return awaitShutdownMillis;
  }

  public int getDefaultMaxQueryParallelism()
  {
    return defaultMaxQueryParallelism;
  }

  public int getTargetRunTimeMillis()
  {
    return targetRunTimeMillis;
  }

  public int getInitialYieldNumRows()
  {
    return initialYieldNumRows;
  }

  public int getSmallBatchNumRows()
  {
    return smallBatchNumRows;
  }
}
