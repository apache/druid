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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName("approxHistogramFold")
public class ApproximateHistogramFoldingAggregatorFactory extends ApproximateHistogramAggregatorFactory
{

  @JsonCreator
  public ApproximateHistogramFoldingAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("resolution") Integer resolution,
      @JsonProperty("numBuckets") Integer numBuckets,
      @JsonProperty("lowerLimit") Float lowerLimit,
      @JsonProperty("upperLimit") Float upperLimit,
      @JsonProperty("finalizeAsBase64Binary") @Nullable Boolean finalizeAsBase64Binary
  )
  {
    super(name, fieldName, resolution, numBuckets, lowerLimit, upperLimit, finalizeAsBase64Binary);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    @SuppressWarnings("unchecked")
    ColumnValueSelector<ApproximateHistogram> selector = metricFactory.makeColumnValueSelector(fieldName);

    final Class cls = selector.classOfObject();
    if (cls.equals(Object.class) || ApproximateHistogram.class.isAssignableFrom(cls)) {
      return new ApproximateHistogramFoldingAggregator(
          selector,
          resolution,
          lowerLimit,
          upperLimit
      );
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a ApproximateHistogram, got a %s",
        fieldName,
        cls
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    @SuppressWarnings("unchecked")
    ColumnValueSelector<ApproximateHistogram> selector = metricFactory.makeColumnValueSelector(fieldName);

    final Class cls = selector.classOfObject();
    if (cls.equals(Object.class) || ApproximateHistogram.class.isAssignableFrom(cls)) {
      return new ApproximateHistogramFoldingBufferAggregator(selector, resolution, lowerLimit, upperLimit);
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a ApproximateHistogram, got a %s",
        fieldName,
        cls
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ApproximateHistogramFoldingAggregatorFactory(name, name, resolution, numBuckets, lowerLimit, upperLimit, finalizeAsBase64Binary);
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(AggregatorUtil.APPROX_HIST_FOLDING_CACHE_TYPE_ID)
        .appendString(fieldName)
        .appendInt(resolution)
        .appendInt(numBuckets)
        .appendFloat(lowerLimit)
        .appendFloat(upperLimit)
        .appendBoolean(finalizeAsBase64Binary);

    return builder.build();
  }

  @Override
  public String toString()
  {
    return "ApproximateHistogramFoldingAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", resolution=" + resolution +
           ", numBuckets=" + numBuckets +
           ", lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           ", finalizeAsBase64Binary=" + finalizeAsBase64Binary +
           '}';
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
    ApproximateHistogramAggregatorFactory that = (ApproximateHistogramAggregatorFactory) o;
    return resolution == that.resolution &&
           numBuckets == that.numBuckets &&
           Float.compare(that.lowerLimit, lowerLimit) == 0 &&
           Float.compare(that.upperLimit, upperLimit) == 0 &&
           finalizeAsBase64Binary == that.finalizeAsBase64Binary &&
           Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, resolution, numBuckets, lowerLimit, upperLimit, finalizeAsBase64Binary);
  }
}

