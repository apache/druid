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
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("quantiles")
public class QuantilesPostAggregator extends ApproximateHistogramPostAggregator
{
  private final float[] probabilities;

  @JsonCreator
  public QuantilesPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probabilities") float[] probabilities
  )
  {
    super(name, fieldName);
    this.probabilities = probabilities;

    for (float p : probabilities) {
      if (p < 0 || p > 1) {
        throw new IAE("Illegal probability[%s], must be strictly between 0 and 1", p);
      }
    }
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    Object val = values.get(fieldName);
    if (val instanceof ApproximateHistogram) {
      final ApproximateHistogram ah = (ApproximateHistogram) val;
      return new Quantiles(probabilities, ah.getQuantiles(probabilities), ah.getMin(), ah.getMax());
    } else if (val instanceof FixedBucketsHistogram) {
      final FixedBucketsHistogram fbh = (FixedBucketsHistogram) val;
      double[] adjustedProbabilites = new double[probabilities.length];
      for (int i = 0; i < probabilities.length; i++) {
        adjustedProbabilites[i] = probabilities[i] * 100.0;
      }
      return new Quantiles(
          probabilities,
          fbh.percentilesFloat(adjustedProbabilites),
          (float) fbh.getMin(),
          (float) fbh.getMax()
      );
    }
    throw new ISE("Unknown value type: " + val.getClass());
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty
  public float[] getProbabilities()
  {
    return probabilities;
  }

  @Override
  public String toString()
  {
    return "QuantilesPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", probabilities=" + Arrays.toString(this.getProbabilities()) +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.HISTOGRAM_QUANTILES)
        .appendString(fieldName)
        .appendFloatArray(probabilities)
        .build();
  }
}
