/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.topn;

import io.druid.query.QueryMetrics;

/**
 * Specialization of {@link QueryMetrics} for {@link TopNQuery}.
 */
public interface TopNQueryMetrics extends QueryMetrics<TopNQuery>
{
  /**
   * Sets {@link TopNQuery#getThreshold()} of the given query as dimension.
   */
  void threshold(TopNQuery query);

  /**
   * Sets {@link TopNQuery#getDimensionSpec()}.{@link io.druid.query.dimension.DimensionSpec#getDimension()
   * getDimension()} of the given query as dimension.
   */
  void dimension(TopNQuery query);

  /**
   * Sets the number of metrics of the given topN query as dimension.
   */
  void numMetrics(TopNQuery query);

  /**
   * Sets the number of "complex" metrics of the given topN query as dimension. By default it is assumed that "complex"
   * metric is a metric of not long or double type, but it could be redefined in the implementation of this method.
   */
  void numComplexMetrics(TopNQuery query);
}
