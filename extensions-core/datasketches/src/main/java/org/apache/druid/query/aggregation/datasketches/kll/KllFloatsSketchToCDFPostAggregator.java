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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.druid.java.util.common.IAE;
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

public class KllFloatsSketchToCDFPostAggregator implements PostAggregator
{

  private final String name;
  private final PostAggregator field;
  private final float[] splitPoints;

  @JsonCreator
  public KllFloatsSketchToCDFPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("splitPoints") final float[] splitPoints)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.splitPoints = Preconditions.checkNotNull(splitPoints, "array of split points is null");
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final KllFloatsSketch sketch = (KllFloatsSketch) field.compute(combinedAggregators);
    if (sketch.isEmpty()) {
      final double[] cdf = new double[splitPoints.length + 1];
      Arrays.fill(cdf, Double.NaN);
      return cdf;
    }
    return sketch.getCDF(splitPoints);
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
  public float[] getSplitPoints()
  {
    return splitPoints;
  }

  @Override
  public Comparator<double[]> getComparator()
  {
    throw new IAE("Comparing histograms is not supported");
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
        ", splitPoints=" + Arrays.toString(splitPoints) +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KllFloatsSketchToCDFPostAggregator that = (KllFloatsSketchToCDFPostAggregator) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (!Arrays.equals(splitPoints, that.splitPoints)) {
      return false;
    }
    return field.equals(that.field);
  }

  @Override
  public int hashCode()
  {
    int hashCode = name.hashCode() * 31 + field.hashCode();
    hashCode = hashCode * 31 + Arrays.hashCode(splitPoints);
    return hashCode;
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(
        PostAggregatorIds.KLL_FLOATS_SKETCH_TO_CDF_CACHE_TYPE_ID).appendCacheable(field);
    for (final float value : splitPoints) {
      builder.appendFloat(value);
    }
    return builder.build();
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
  }

}
