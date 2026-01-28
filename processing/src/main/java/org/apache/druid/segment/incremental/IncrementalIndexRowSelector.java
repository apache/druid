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

import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnFormat;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface that abstracts selecting data from a {@link FactsHolder}
 */
public interface IncrementalIndexRowSelector extends ColumnInspector
{
  /**
   * Returns list of {@link IncrementalIndex.DimensionDesc} for the row selector
   */
  List<IncrementalIndex.DimensionDesc> getDimensions();

  /**
   * Returns list of dimension names for the row selector, optionally including the time column. If time is included,
   * the order columns appear in this list will match {@link #getOrdering()}
   */
  List<String> getDimensionNames(boolean includeTime);

  /**
   * Returns list of all metric column names for the row selector
   */
  List<String> getMetricNames();

  /**
   * get {@link IncrementalIndex.DimensionDesc} for the specified column, if available, which provides access to things
   * like {@link org.apache.druid.segment.DimensionIndexer} and {@link org.apache.druid.segment.DimensionHandler} as
   * well as column capabilities and position within the row
   */
  @Nullable
  IncrementalIndex.DimensionDesc getDimension(String columnName);

  /**
   * Get {@link IncrementalIndex.MetricDesc} which provides column capabilities and position in the aggregators section
   * of the row
   */
  @Nullable
  IncrementalIndex.MetricDesc getMetric(String s);

  /**
   * Get {@link ColumnFormat} for a dimension, metrics, or time column, or null if the column does not exist
   */
  @Nullable
  ColumnFormat getColumnFormat(String columnName);

  /**
   * Ordering for the data in the facts table
   */
  List<OrderBy> getOrdering();

  /**
   * Position of the time column in {@link #getOrdering()}
   */
  int getTimePosition();

  /**
   * Are there any {@link IncrementalIndexRow} stored in the {@link FactsHolder}?
   */
  boolean isEmpty();

  /**
   * Number of rows in {@link FactsHolder}
   */
  int numRows();

  /**
   * Get the {@link FactsHolder} containing all of the {@link IncrementalIndexRow} backing this selector
   */
  FactsHolder getFacts();

  /**
   * Highest value {@link IncrementalIndexRow#getRowIndex()} available in this selector. Note that these values do not
   * reflect the position of the row in the {@link FactsHolder}, rather just the order in which they were processed
   */
  int getLastRowIndex();

  /**
   * @param rowOffset row to get float aggregator value
   * @param aggOffset position of the aggregator in the aggregators array of the data schema
   * @return          float value of the metric
   */
  float getMetricFloatValue(int rowOffset, int aggOffset);

  /**
   * @param rowOffset row to get long aggregator value
   * @param aggOffset position of the aggregator in the aggregators array of the data schema
   * @return          long value of the aggregator for this row
   */
  long getMetricLongValue(int rowOffset, int aggOffset);

  /**
   * @param rowOffset row to get double aggregator value
   * @param aggOffset position of the aggregator in the aggregators array of the data schema
   * @return          double value of the aggregator for this row
   */
  double getMetricDoubleValue(int rowOffset, int aggOffset);

  /**
   * @param rowOffset row to get long aggregator value
   * @param aggOffset position of the aggregator in the aggregators array of the data schema
   * @return          long value of the aggregator for this row
   */
  @Nullable
  Object getMetricObjectValue(int rowOffset, int aggOffset);

  /**
   * @param rowOffset row to check for a aggregator value
   * @param aggOffset position of the aggregator in the aggregators array of the data schema
   * @return          is the value null for this row?
   */
  boolean isNull(int rowOffset, int aggOffset);
}
