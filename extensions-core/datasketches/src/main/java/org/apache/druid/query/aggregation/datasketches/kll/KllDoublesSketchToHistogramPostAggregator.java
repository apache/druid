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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class KllDoublesSketchToHistogramPostAggregator implements PostAggregator
{
  static final int DEFAULT_NUM_BINS = 10;

  private final String name;
  private final PostAggregator field;
  private final double[] splitPoints;
  private final Integer numBins;

  @JsonCreator
  public KllDoublesSketchToHistogramPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("splitPoints") @Nullable final double[] splitPoints,
      @JsonProperty("numBins") @Nullable final Integer numBins)
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.field = Preconditions.checkNotNull(field, "field is null");
    this.splitPoints = splitPoints;
    this.numBins = numBins;
    if (splitPoints != null && numBins != null) {
      throw new IAE("Cannot accept both 'splitPoints' and 'numBins'");
    }
  }

  @Override
  public Object compute(final Map<String, Object> combinedAggregators)
  {
    final KllDoublesSketch sketch = (KllDoublesSketch) field.compute(combinedAggregators);
    final int numBins = this.splitPoints != null ? this.splitPoints.length + 1 :
        (this.numBins != null ? this.numBins.intValue() : DEFAULT_NUM_BINS);
    if (numBins < 2) {
      throw new IAE("at least 2 bins expected");
    }
    if (sketch.isEmpty()) {
      final double[] histogram = new double[numBins];
      Arrays.fill(histogram, Double.NaN);
      return histogram;
    }
    final double[] splitPoints;
    if (this.splitPoints != null) {
      splitPoints = this.splitPoints;
    } else {
      final double min = sketch.getMinItem();
      final double max = sketch.getMaxItem();
      if (min == max) {
        // if min is equal to max, we can't create an array of equally spaced points.
        // all values would go into the first bucket anyway, and the remaining
        // buckets are left as zero.
        final double[] histogram = new double[numBins];
        histogram[0] = sketch.getN();
        return histogram;
      }
      splitPoints = equallySpacedPoints(numBins, min, max);
    }
    final double[] histogram = sketch.getPMF(splitPoints);
    for (int i = 0; i < histogram.length; i++) {
      histogram[i] *= sketch.getN(); // scale fractions to counts
    }
    return histogram;
  }

  // retuns num-1 points that split the interval [min, max] into num equally-spaced intervals
  // num must be at least 2
  private static double[] equallySpacedPoints(final int num, final double min, final double max)
  {
    final double[] points = new double[num - 1];
    final double delta = (max - min) / num;
    for (int i = 0; i < num - 1; i++) {
      points[i] = min + delta * (i + 1);
    }
    return points;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  /**
   * actual type is {@link KllDoublesSketch}
   * @param signature
   */
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public double[] getSplitPoints()
  {
    return splitPoints;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getNumBins()
  {
    return numBins;
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
        ", numBins=" + numBins +
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
    final KllDoublesSketchToHistogramPostAggregator that = (KllDoublesSketchToHistogramPostAggregator) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (!Arrays.equals(splitPoints, that.splitPoints)) {
      return false;
    }
    if (!field.equals(that.field)) {
      return false;
    }
    if (numBins == null && that.numBins == null) {
      return true;
    }
    if (numBins != null && numBins.equals(that.numBins)) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    int hashCode = name.hashCode() * 31 + field.hashCode();
    hashCode = hashCode * 31 + Arrays.hashCode(splitPoints);
    if (numBins != null) {
      hashCode = hashCode * 31 + numBins.hashCode();
    }
    return hashCode;
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(
        PostAggregatorIds.KLL_DOUBLES_SKETCH_TO_HISTOGRAM_CACHE_TYPE_ID).appendCacheable(field);
    if (splitPoints != null) {
      for (final double value : splitPoints) {
        builder.appendDouble(value);
      }
    }
    if (numBins != null) {
      builder.appendInt(numBins);
    }
    return builder.build();
  }

  @Override
  public PostAggregator decorate(final Map<String, AggregatorFactory> map)
  {
    return this;
  }

}
