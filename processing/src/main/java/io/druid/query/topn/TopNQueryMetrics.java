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
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;

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

  void dimensionCardinality(int cardinality);

  void algorithm(TopNAlgorithm algorithm);

  /**
   * This method is called exactly once with each cursor, processed for the query.
   */
  void cursor(Cursor cursor);

  /**
   * This method is called exactly once with the columnValueSelector object of each cursor, processed for the query.
   */
  void columnValueSelector(ColumnValueSelector columnValueSelector);

  /**
   * This method may set {@link TopNParams#getNumValuesPerPass()} of the query as dimension.
   */
  void numValuesPerPass(TopNParams params);

  /**
   * Called with the number of rows, processed via each cursor, processed for the query within the segment. The total
   * number of processed rows, reported via this method for a TopNQueryMetrics instance, is smaller or equal to
   * {@link #reportPreFilteredRows(long)}, because {@link #postFilters} are additionally applied. If there
   * are no postFilters, preFilteredRows and processedRows are equal.
   */
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
