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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;

import javax.annotation.Nullable;

public class DoublesSketchMergeAggregatorFactory extends DoublesSketchAggregatorFactory
{

  @JsonCreator
  public DoublesSketchMergeAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("k") @Nullable final Integer k,
      @JsonProperty("maxStreamLength") @Nullable final Long maxStreamLength
  )
  {
    super(name, name, k, maxStreamLength, AggregatorUtil.QUANTILES_DOUBLES_SKETCH_MERGE_CACHE_TYPE_ID);
  }

  @VisibleForTesting
  DoublesSketchMergeAggregatorFactory(
      final String name,
      @Nullable final Integer k
  )
  {
    this(name, k, null);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(getFieldName());
    if (selector instanceof NilColumnValueSelector) {
      return new NoopDoublesSketchAggregator();
    }
    return new DoublesSketchMergeAggregator(selector, getK());
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(getFieldName());
    if (selector instanceof NilColumnValueSelector) {
      return new NoopDoublesSketchBufferAggregator();
    }
    return new DoublesSketchMergeBufferAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DoublesSketchMergeAggregatorFactory(newName, getK(), getMaxStreamLength());
  }
}
