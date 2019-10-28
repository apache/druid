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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;

/**
 */
public class GroupByQueryConfig
{
  public static final String CTX_KEY_STRATEGY = "groupByStrategy";
  public static final String CTX_KEY_FORCE_LIMIT_PUSH_DOWN = "forceLimitPushDown";
  public static final String CTX_KEY_APPLY_LIMIT_PUSH_DOWN = "applyLimitPushDown";
  public static final String CTX_KEY_APPLY_LIMIT_PUSH_DOWN_TO_SEGMENT = "applyLimitPushDownToSegment";
  public static final String CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY = "forcePushDownNestedQuery";
  public static final String CTX_KEY_EXECUTING_NESTED_QUERY = "executingNestedQuery";
  public static final String CTX_KEY_ARRAY_RESULT_ROWS = "resultAsArray";
  private static final String CTX_KEY_IS_SINGLE_THREADED = "groupByIsSingleThreaded";
  private static final String CTX_KEY_MAX_INTERMEDIATE_ROWS = "maxIntermediateRows";
  private static final String CTX_KEY_MAX_RESULTS = "maxResults";
  private static final String CTX_KEY_BUFFER_GROUPER_INITIAL_BUCKETS = "bufferGrouperInitialBuckets";
  private static final String CTX_KEY_BUFFER_GROUPER_MAX_LOAD_FACTOR = "bufferGrouperMaxLoadFactor";
  private static final String CTX_KEY_BUFFER_GROUPER_MAX_SIZE = "bufferGrouperMaxSize";
  private static final String CTX_KEY_MAX_ON_DISK_STORAGE = "maxOnDiskStorage";
  private static final String CTX_KEY_MAX_MERGING_DICTIONARY_SIZE = "maxMergingDictionarySize";
  private static final String CTX_KEY_FORCE_HASH_AGGREGATION = "forceHashAggregation";
  private static final String CTX_KEY_INTERMEDIATE_COMBINE_DEGREE = "intermediateCombineDegree";
  private static final String CTX_KEY_NUM_PARALLEL_COMBINE_THREADS = "numParallelCombineThreads";
  public static final String CTX_KEY_VECTORIZE = "vectorize";

  @JsonProperty
  private String defaultStrategy = GroupByStrategySelector.STRATEGY_V2;

  @JsonProperty
  private boolean singleThreaded = false;

  @JsonProperty
  private int maxIntermediateRows = 50000;

  @JsonProperty
  private int maxResults = 500000;

  @JsonProperty
  // Not documented, only used for tests to force spilling
  private int bufferGrouperMaxSize = Integer.MAX_VALUE;

  @JsonProperty
  private float bufferGrouperMaxLoadFactor = 0;

  @JsonProperty
  private int bufferGrouperInitialBuckets = 0;

  @JsonProperty
  // Size of on-heap string dictionary for merging, per-query; when exceeded, partial results will be spilled to disk
  private long maxMergingDictionarySize = 100_000_000L;

  @JsonProperty
  // Max on-disk temporary storage, per-query; when exceeded, the query fails
  private long maxOnDiskStorage = 0L;

  @JsonProperty
  private boolean forcePushDownLimit = false;

  @JsonProperty
  private boolean applyLimitPushDownToSegment = true;

  @JsonProperty
  private boolean forcePushDownNestedQuery = false;

  @JsonProperty
  private boolean forceHashAggregation = false;

  @JsonProperty
  private int intermediateCombineDegree = 8;

  @JsonProperty
  private int numParallelCombineThreads = 1;

  @JsonProperty
  private boolean vectorize = false;

  public String getDefaultStrategy()
  {
    return defaultStrategy;
  }

  public boolean isSingleThreaded()
  {
    return singleThreaded;
  }

  public void setSingleThreaded(boolean singleThreaded)
  {
    this.singleThreaded = singleThreaded;
  }

  public int getMaxIntermediateRows()
  {
    return maxIntermediateRows;
  }

  public void setMaxIntermediateRows(int maxIntermediateRows)
  {
    this.maxIntermediateRows = maxIntermediateRows;
  }

  public int getMaxResults()
  {
    return maxResults;
  }

  public void setMaxResults(int maxResults)
  {
    this.maxResults = maxResults;
  }

  public int getBufferGrouperMaxSize()
  {
    return bufferGrouperMaxSize;
  }

  public float getBufferGrouperMaxLoadFactor()
  {
    return bufferGrouperMaxLoadFactor;
  }

  public int getBufferGrouperInitialBuckets()
  {
    return bufferGrouperInitialBuckets;
  }

  public long getMaxMergingDictionarySize()
  {
    return maxMergingDictionarySize;
  }

  public long getMaxOnDiskStorage()
  {
    return maxOnDiskStorage;
  }

  public boolean isForcePushDownLimit()
  {
    return forcePushDownLimit;
  }

  public boolean isApplyLimitPushDownToSegment()
  {
    return applyLimitPushDownToSegment;
  }

  public boolean isForceHashAggregation()
  {
    return forceHashAggregation;
  }

  public int getIntermediateCombineDegree()
  {
    return intermediateCombineDegree;
  }

  public int getNumParallelCombineThreads()
  {
    return numParallelCombineThreads;
  }

  public boolean isVectorize()
  {
    return vectorize;
  }

  public boolean isForcePushDownNestedQuery()
  {
    return forcePushDownNestedQuery;
  }

  public GroupByQueryConfig withOverrides(final GroupByQuery query)
  {
    final GroupByQueryConfig newConfig = new GroupByQueryConfig();
    newConfig.defaultStrategy = query.getContextValue(CTX_KEY_STRATEGY, getDefaultStrategy());
    newConfig.singleThreaded = query.getContextBoolean(CTX_KEY_IS_SINGLE_THREADED, isSingleThreaded());
    newConfig.maxIntermediateRows = Math.min(
        query.getContextValue(CTX_KEY_MAX_INTERMEDIATE_ROWS, getMaxIntermediateRows()),
        getMaxIntermediateRows()
    );
    newConfig.maxResults = Math.min(
        query.getContextValue(CTX_KEY_MAX_RESULTS, getMaxResults()),
        getMaxResults()
    );
    newConfig.bufferGrouperMaxSize = Math.min(
        query.getContextValue(CTX_KEY_BUFFER_GROUPER_MAX_SIZE, getBufferGrouperMaxSize()),
        getBufferGrouperMaxSize()
    );
    newConfig.bufferGrouperMaxLoadFactor = query.getContextValue(
        CTX_KEY_BUFFER_GROUPER_MAX_LOAD_FACTOR,
        getBufferGrouperMaxLoadFactor()
    );
    newConfig.bufferGrouperInitialBuckets = query.getContextValue(
        CTX_KEY_BUFFER_GROUPER_INITIAL_BUCKETS,
        getBufferGrouperInitialBuckets()
    );
    newConfig.maxOnDiskStorage = Math.min(
        ((Number) query.getContextValue(CTX_KEY_MAX_ON_DISK_STORAGE, getMaxOnDiskStorage())).longValue(),
        getMaxOnDiskStorage()
    );
    newConfig.maxMergingDictionarySize = Math.min(
        ((Number) query.getContextValue(
            CTX_KEY_MAX_MERGING_DICTIONARY_SIZE,
            getMaxMergingDictionarySize()
        )).longValue(),
        getMaxMergingDictionarySize()
    );
    newConfig.forcePushDownLimit = query.getContextBoolean(CTX_KEY_FORCE_LIMIT_PUSH_DOWN, isForcePushDownLimit());
    newConfig.applyLimitPushDownToSegment = query.getContextBoolean(
        CTX_KEY_APPLY_LIMIT_PUSH_DOWN_TO_SEGMENT,
        isApplyLimitPushDownToSegment()
    );
    newConfig.forceHashAggregation = query.getContextBoolean(CTX_KEY_FORCE_HASH_AGGREGATION, isForceHashAggregation());
    newConfig.forcePushDownNestedQuery = query.getContextBoolean(CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, isForcePushDownNestedQuery());
    newConfig.intermediateCombineDegree = query.getContextValue(
        CTX_KEY_INTERMEDIATE_COMBINE_DEGREE,
        getIntermediateCombineDegree()
    );
    newConfig.numParallelCombineThreads = query.getContextValue(
        CTX_KEY_NUM_PARALLEL_COMBINE_THREADS,
        getNumParallelCombineThreads()
    );
    newConfig.vectorize = query.getContextBoolean(CTX_KEY_VECTORIZE, isVectorize());
    return newConfig;
  }

  @Override
  public String toString()
  {
    return "GroupByQueryConfig{" +
           "defaultStrategy='" + defaultStrategy + '\'' +
           ", singleThreaded=" + singleThreaded +
           ", maxIntermediateRows=" + maxIntermediateRows +
           ", maxResults=" + maxResults +
           ", bufferGrouperMaxSize=" + bufferGrouperMaxSize +
           ", bufferGrouperMaxLoadFactor=" + bufferGrouperMaxLoadFactor +
           ", bufferGrouperInitialBuckets=" + bufferGrouperInitialBuckets +
           ", maxMergingDictionarySize=" + maxMergingDictionarySize +
           ", maxOnDiskStorage=" + maxOnDiskStorage +
           ", forcePushDownLimit=" + forcePushDownLimit +
           ", forceHashAggregation=" + forceHashAggregation +
           ", intermediateCombineDegree=" + intermediateCombineDegree +
           ", numParallelCombineThreads=" + numParallelCombineThreads +
           ", vectorize=" + vectorize +
           ", forcePushDownNestedQuery=" + forcePushDownNestedQuery +
           '}';
  }
}
