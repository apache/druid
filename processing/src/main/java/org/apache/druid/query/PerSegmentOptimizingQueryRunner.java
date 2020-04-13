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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;

/**
 * This runner optimizes queries made on a single segment, using per-segment information,
 * before submitting the queries to the base runner.
 *
 * Example optimizations include adjusting query filters based on per-segment information, such as intervals.
 *
 * This query runner should only wrap base query runners that will
 * be used to query a single segment (i.e., when the query reaches a historical node).
 *
 * @param <T>
 */
public class PerSegmentOptimizingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> base;
  private final PerSegmentQueryOptimizationContext optimizationContext;

  public PerSegmentOptimizingQueryRunner(
      QueryRunner<T> base,
      PerSegmentQueryOptimizationContext optimizationContext
  )
  {
    this.base = base;
    this.optimizationContext = optimizationContext;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> input, final ResponseContext responseContext)
  {
    return base.run(
        input.optimizeForSegment(optimizationContext),
        responseContext
    );
  }
}
