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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Returns a distinct count estimate and error bounds from a given {@link HllSketch}.
 * The result will be three double values: estimate, lower bound and upper bound.
 * The bounds are provided at a given number of standard deviations (optional, defaults to 1).
 * This must be an integer value of 1, 2 or 3 corresponding to approximately 68.3%, 95.4% and 99.7%
 * confidence intervals.
 */
public class HllSketchToEstimateWithBoundsPostAggregator implements PostAggregator
{
  public static final int DEFAULT_NUM_STD_DEVS = 1;

  private final String name;
  private final PostAggregator field;
  private final int numStdDevs;

  @JsonCreator
  public HllSketchToEstimateWithBoundsPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("numStdDev") @Nullable final Integer numStdDevs
  )
  {
    this.name = name;
    this.field = field;
    this.numStdDevs = numStdDevs == null ? DEFAULT_NUM_STD_DEVS : numStdDevs;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public int getNumStdDev()
  {
    return numStdDevs;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public Comparator<double[]> getComparator()
  {
    throw new IAE("Comparing arrays of estimates and error bounds is not supported");
  }

  @Override
  public double[] compute(final Map<String, Object> combinedAggregators)
  {
    final HllSketch sketch = (HllSketch) field.compute(combinedAggregators);
    return new double[] {sketch.getEstimate(), sketch.getLowerBound(numStdDevs), sketch.getUpperBound(numStdDevs)};
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
        "name='" + name + '\'' +
        ", field=" + field +
        ", numStdDev=" + numStdDevs +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HllSketchToEstimateWithBoundsPostAggregator)) {
      return false;
    }

    final HllSketchToEstimateWithBoundsPostAggregator that = (HllSketchToEstimateWithBoundsPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (numStdDevs != that.numStdDevs) {
      return false;
    }
    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, numStdDevs);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.HLL_SKETCH_TO_ESTIMATE_AND_BOUNDS_CACHE_TYPE_ID)
        .appendString(name)
        .appendCacheable(field)
        .appendInt(numStdDevs)
        .build();
  }

}
