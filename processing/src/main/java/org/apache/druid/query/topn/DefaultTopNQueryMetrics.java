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

import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;

public class DefaultTopNQueryMetrics extends DefaultQueryMetrics<TopNQuery> implements TopNQueryMetrics
{
  @Override
  public void query(TopNQuery query)
  {
    super.query(query);
    threshold(query);
    dimension(query);
    numMetrics(query);
    numComplexMetrics(query);
    granularity(query);
  }

  @Override
  public void threshold(TopNQuery query)
  {
    setDimension("threshold", String.valueOf(query.getThreshold()));
  }

  @Override
  public void dimension(TopNQuery query)
  {
    setDimension("dimension", query.getDimensionSpec().getDimension());
  }

  @Override
  public void numMetrics(TopNQuery query)
  {
    setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()));
  }

  @Override
  public void numComplexMetrics(TopNQuery query)
  {
    int numComplexAggs = DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs());
    setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
  }

  @Override
  public void granularity(TopNQuery query)
  {
    // Don't emit by default
  }

  @Override
  public void dimensionCardinality(int cardinality)
  {
    // Don't emit by default.
  }

  @Override
  public void algorithm(TopNAlgorithm algorithm)
  {
    // Emit nothing by default.
  }

  @Override
  public void cursor(Cursor cursor)
  {
    // Emit nothing by default.
  }

  @Override
  public void columnValueSelector(ColumnValueSelector columnValueSelector)
  {
    // Emit nothing by default.
  }

  @Override
  public void numValuesPerPass(TopNParams params)
  {
    // Don't emit by default.
  }

  @Override
  public TopNQueryMetrics addProcessedRows(long numRows)
  {
    // Emit nothing by default.
    return this;
  }

  @Override
  public void startRecordingScanTime()
  {
    // Don't record scan time by default.
  }

  @Override
  public TopNQueryMetrics stopRecordingScanTime()
  {
    // Emit nothing by default.
    return this;
  }
}
