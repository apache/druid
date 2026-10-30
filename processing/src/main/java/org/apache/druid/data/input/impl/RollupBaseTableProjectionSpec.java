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

package org.apache.druid.data.input.impl;

import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;

/**
 * A {@link BaseTableProjectionSpec} for a table that aggregates the rows it stores, and so has metric columns as well
 * as the columns it groups by.
 * <p>
 * This exists so that the parts of the system which can supply metrics, notably compaction absorbing the schema of the
 * segments it is rewriting, can hand them to a spec that is able to store them without every non-rollup spec having to
 * carry an API for aggregators it has no use for.
 */
public interface RollupBaseTableProjectionSpec extends BaseTableProjectionSpec
{
  @Override
  default boolean isRollup()
  {
    return true;
  }

  /**
   * Returns a copy of this spec with the given aggregators appended to those it already declares, the metric-column
   * counterpart of {@link #withAdditionalColumns}. Each aggregator must combine its own output, which is what the
   * aggregators recovered from existing segments already do.
   */
  RollupBaseTableProjectionSpec withAdditionalMetrics(@Nullable AggregatorFactory[] additionalMetrics);
}
