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

package org.apache.druid.query.topn;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;

/**
 * Specialization of {@link QueryMetrics} for {@link TopNQuery}.
 */
@ExtensionPoint
public interface TopNQueryMetrics extends QueryMetrics<TopNQuery>
{
  /**
   * Sets {@link TopNQuery#getThreshold()} of the given query as dimension.
   */
  @PublicApi
  void threshold(TopNQuery query);

  /**
   * Sets {@link TopNQuery#getDimensionSpec()}.{@link org.apache.druid.query.dimension.DimensionSpec#getDimension()
   * getDimension()} of the given query as dimension.
   */
  @PublicApi
  void dimension(TopNQuery query);

  /**
   * Sets the number of metrics of the given topN query as dimension.
   */
  @PublicApi
  void numMetrics(TopNQuery query);

  /**
   * Sets the number of "complex" metrics of the given topN query as dimension. By default it is assumed that "complex"
   * metric is a metric of not long or double type, but it could be redefined in the implementation of this method.
   */
  @PublicApi
  void numComplexMetrics(TopNQuery query);

  /**
   * Sets the granularity of {@link TopNQuery#getGranularity()} of the given query as dimension.
   */
  @PublicApi
  void granularity(TopNQuery query);

  @PublicApi
  void dimensionCardinality(int cardinality);

  @PublicApi
  void algorithm(TopNAlgorithm algorithm);

  /**
   * This method is called exactly once with each cursor, processed for the query.
   */
  @PublicApi
  void cursor(Cursor cursor);

  /**
   * This method is called exactly once with the columnValueSelector object of each cursor, processed for the query.
   */
  @PublicApi
  void columnValueSelector(ColumnValueSelector columnValueSelector);

  /**
   * This method may set {@link TopNParams#getNumValuesPerPass()} of the query as dimension.
   */
  @PublicApi
  void numValuesPerPass(TopNParams params);

  /**
   * Called with the number of rows, processed via each cursor, processed for the query within the segment. The total
   * number of processed rows, reported via this method for a TopNQueryMetrics instance, is smaller or equal to
   * {@link #reportPreFilteredRows(long)}, because {@link #postFilters} are additionally applied. If there
   * are no postFilters, preFilteredRows and processedRows are equal.
   */
  @PublicApi
  TopNQueryMetrics addProcessedRows(long numRows);

  /**
   * Calls to this method and {@link #stopRecordingScanTime()} wrap scanning of each cursor, processed for the
   * query.
   */
  void startRecordingScanTime();

  /**
   * Calls of {@link #startRecordingScanTime()} and this method wrap scanning of each cursor, processed for the query.
   */
  TopNQueryMetrics stopRecordingScanTime();
}
