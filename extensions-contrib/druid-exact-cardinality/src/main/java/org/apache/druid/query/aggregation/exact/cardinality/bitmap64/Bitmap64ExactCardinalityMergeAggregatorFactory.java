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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

public class Bitmap64ExactCardinalityMergeAggregatorFactory extends Bitmap64ExactCardinalityAggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(Bitmap64ExactCardinalityModule.MERGE_TYPE_NAME);

  @JsonCreator
  public Bitmap64ExactCardinalityMergeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    super(name, fieldName);
  }

  @Override
  protected byte getCacheTypeId()
  {
    return AggregatorUtil.BITMAP64_EXACT_COUNT_MERGE_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ColumnValueSelector<Bitmap64Counter> selector = metricFactory.makeColumnValueSelector(getFieldName());
    return new Bitmap64ExactCardinalityMergeAggregator(selector);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ColumnValueSelector<Bitmap64Counter> selector = metricFactory.makeColumnValueSelector(getFieldName());
    return new Bitmap64ExactCardinalityMergeBufferAggregator(selector);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }
}
