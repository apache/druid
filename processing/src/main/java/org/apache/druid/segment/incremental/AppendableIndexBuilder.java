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

package org.apache.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;

public abstract class AppendableIndexBuilder
{
  @Nullable
  protected IncrementalIndexSchema incrementalIndexSchema = null;
  protected int maxRowCount = 0;
  protected long maxBytesInMemory = 0;
  // When set to true, for any row that already has metric (with the same name defined in metricSpec),
  // the metric aggregator in metricSpec is skipped and the existing metric is unchanged. If the row does not already have
  // the metric, then the metric aggregator is applied on the source column as usual. This should only be set for
  // DruidInputSource since that is the only case where we can have existing metrics.
  // This is currently only use by auto compaction and should not be use for anything else.
  protected boolean preserveExistingMetrics = false;
  protected boolean useMaxMemoryEstimates = true;

  protected final Logger log = new Logger(this.getClass());

  public AppendableIndexBuilder setIndexSchema(final IncrementalIndexSchema incrementalIndexSchema)
  {
    this.incrementalIndexSchema = incrementalIndexSchema;
    return this;
  }

  /**
   * A helper method to set a simple index schema with only metrics and default values for the other parameters. Note
   * that this method is normally used for testing and benchmarking; it is unlikely that you would use it in
   * production settings.
   *
   * @param metrics variable array of {@link AggregatorFactory} metrics
   *
   * @return this
   */
  @VisibleForTesting
  public AppendableIndexBuilder setSimpleTestingIndexSchema(final AggregatorFactory... metrics)
  {
    return setSimpleTestingIndexSchema(null, null, metrics);
  }


  /**
   * A helper method to set a simple index schema with controllable metrics and rollup, and default values for the
   * other parameters. Note that this method is normally used for testing and benchmarking; it is unlikely that you
   * would use it in production settings.
   *
   * @param metrics variable array of {@link AggregatorFactory} metrics
   *
   * @return this
   */
  @VisibleForTesting
  public AppendableIndexBuilder setSimpleTestingIndexSchema(
      @Nullable Boolean rollup,
      @Nullable Boolean preserveExistingMetrics,
      final AggregatorFactory... metrics
  )
  {
    IncrementalIndexSchema.Builder builder = new IncrementalIndexSchema.Builder().withMetrics(metrics);
    this.incrementalIndexSchema = rollup != null ? builder.withRollup(rollup).build() : builder.build();
    this.preserveExistingMetrics = preserveExistingMetrics != null ? preserveExistingMetrics : false;
    return this;
  }

  public AppendableIndexBuilder setMaxRowCount(final int maxRowCount)
  {
    this.maxRowCount = maxRowCount;
    return this;
  }

  public AppendableIndexBuilder setMaxBytesInMemory(final long maxBytesInMemory)
  {
    this.maxBytesInMemory = maxBytesInMemory;
    return this;
  }

  public AppendableIndexBuilder setPreserveExistingMetrics(final boolean preserveExistingMetrics)
  {
    this.preserveExistingMetrics = preserveExistingMetrics;
    return this;
  }

  public AppendableIndexBuilder setUseMaxMemoryEstimates(final boolean useMaxMemoryEstimates)
  {
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
    return this;
  }

  public void validate()
  {
    if (maxRowCount <= 0) {
      throw new IllegalArgumentException("Invalid max row count: " + maxRowCount);
    }

    if (incrementalIndexSchema == null) {
      throw new IllegalArgumentException("incrementIndexSchema cannot be null");
    }
  }

  public final IncrementalIndex build()
  {
    log.debug("Building appendable index.");
    validate();
    return buildInner();
  }

  protected abstract IncrementalIndex buildInner();
}
