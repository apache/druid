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
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

/**
 * Returns a distinct count estimate and error bounds from a given {@link ArrayOfDoublesSketch}."
 * The result will be three double values: estimate of the number of distinct keys, lower bound and upper bound.
 * The bounds are provided at the given number of standard deviations (optional, defaults to 1).
 * This must be an integer value of 1, 2 or 3 corresponding to approximately 68.3%, 95.4% and 99.7%
 * confidence intervals.
 */
public class ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator extends ArrayOfDoublesSketchUnaryPostAggregator
{

  private final int numStdDevs;

  @JsonCreator
  public ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("numStdDevs") @Nullable final Integer numStdDevs
  )
  {
    super(name, field);
    this.numStdDevs = numStdDevs == null ? 1 : numStdDevs;
  }

  @JsonProperty
  public int getNumStdDevs()
  {
    return numStdDevs;
  }

  @Override
  public double[] compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    return new double[] {sketch.getEstimate(), sketch.getLowerBound(numStdDevs), sketch.getUpperBound(numStdDevs)};
  }

  @Override
  public Comparator<double[]> getComparator()
  {
    throw new IAE("Comparing arrays of estimates and error bounds is not supported");
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{" +
        "name='" + getName() + '\'' +
        ", field=" + getField() +
        ", numStdDevs=" + numStdDevs +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    if (!(o instanceof ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator)) {
      return false;
    }
    final ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator that = (ArrayOfDoublesSketchToEstimateAndBoundsPostAggregator) o;
    if (numStdDevs != that.numStdDevs) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numStdDevs);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.ARRAY_OF_DOUBLES_SKETCH_TO_ESTIMATE_AND_BOUNDS_CACHE_TYPE_ID)
        .appendCacheable(getField())
        .appendInt(numStdDevs)
        .build();
  }

}
