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

package org.apache.druid.spectator.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class SpectatorHistogramPercentilesPostAggregator implements PostAggregator
{
  private final String name;
  private final PostAggregator field;

  private final double[] percentiles;

  public static final String TYPE_NAME = "percentilesSpectatorHistogram";

  @JsonCreator
  public SpectatorHistogramPercentilesPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("percentiles") final double[] percentiles
  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.percentiles = Preconditions.checkNotNull(percentiles, "array of fractions is null");
    Preconditions.checkArgument(this.percentiles.length >= 1, "Array of percentiles cannot " +
                                                              "be empty");
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.DOUBLE_ARRAY;
  }

  @JsonProperty
  public PostAggregator getField()
  {
    return field;
  }

  @JsonProperty
  public double[] getPercentiles()
  {
    return percentiles;
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final SpectatorHistogram sketch = (SpectatorHistogram) field.compute(combinedAggregators);
    return sketch.getPercentileValues(percentiles);
  }

  @Override
  public Comparator<Object> getComparator()
  {
    return ColumnType.DOUBLE_ARRAY.getStrategy();
  }

  @Override
  public Set<String> getDependentFields()
  {
    return field.getDependentFields();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", percentiles=" + Arrays.toString(percentiles) +
           "}";
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(
        PostAggregatorIds.SPECTATOR_HISTOGRAM_SKETCH_PERCENTILES_CACHE_TYPE_ID).appendCacheable(field);
    for (final double value : percentiles) {
      builder.appendDouble(value);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SpectatorHistogramPercentilesPostAggregator that = (SpectatorHistogramPercentilesPostAggregator) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (!Arrays.equals(percentiles, that.percentiles)) {
      return false;
    }
    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    return (name.hashCode() * 31 + field.hashCode()) * 31 + Arrays.hashCode(percentiles);
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
  }
}
