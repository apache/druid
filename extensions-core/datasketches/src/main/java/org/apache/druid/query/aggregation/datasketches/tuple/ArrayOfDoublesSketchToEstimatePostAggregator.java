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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;

/**
 * Returns a distinct count estimate from a given {@link ArrayOfDoublesSketch}.
 */
public class ArrayOfDoublesSketchToEstimatePostAggregator extends ArrayOfDoublesSketchUnaryPostAggregator
{

  @JsonCreator
  public ArrayOfDoublesSketchToEstimatePostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field
  )
  {
    super(name, field);
  }

  @Override
  public Double compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    return sketch.getEstimate();
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return Comparator.naturalOrder();
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_CACHE_TYPE_ID)
        .appendCacheable(getField())
        .build();
  }

}
