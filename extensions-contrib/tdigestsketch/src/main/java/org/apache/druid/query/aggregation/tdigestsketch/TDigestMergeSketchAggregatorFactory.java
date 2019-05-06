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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * Factory to create {@link TDigestMergeSketchAggregator}
 */
public class TDigestMergeSketchAggregatorFactory extends TDigestBuildSketchAggregatorFactory
{
  public static final String TYPE_NAME = "mergeTDigestSketch";

  @JsonCreator
  public TDigestMergeSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("compression") final Integer compression
  )
  {
    super(name, fieldName, compression, AggregatorUtil.TDIGEST_MERGE_SKETCH_CACHE_TYPE_ID);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector<MergingDigest> selector = metricFactory.makeColumnValueSelector(
        getFieldName());
    return new TDigestMergeSketchAggregator(selector, getCompression());
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    final ColumnValueSelector<MergingDigest> selector = metricFactory.makeColumnValueSelector(
        getFieldName()
    );
    return new TDigestMergeSketchBufferAggregator(selector, getCompression());
  }
}
