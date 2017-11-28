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

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.quantiles.DoublesSketch;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.NilColumnValueSelector;

public class DoublesSketchMergeAggregatorFactory extends DoublesSketchAggregatorFactory
{

  @JsonCreator
  public DoublesSketchMergeAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("k") final Integer k)
  {
    super(name, name, k, AggregatorUtil.QUANTILES_DOUBLES_SKETCH_MERGE_CACHE_TYPE_ID);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(getFieldName());
    if (selector instanceof NilColumnValueSelector) {
      return new DoublesSketchNoOpAggregator();
    }
    return new DoublesSketchMergeAggregator(selector, getK());
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(getFieldName());
    if (selector instanceof NilColumnValueSelector) {
      return new DoublesSketchNoOpBufferAggregator();
    }
    return new DoublesSketchMergeBufferAggregator(selector, getK(), getMaxIntermediateSize());
  }

}
