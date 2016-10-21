/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;

import io.druid.java.util.common.IAE;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("quantiles")
public class QuantilesPostAggregator extends ApproximateHistogramPostAggregator
{
  private final float[] probabilities;
  private String fieldName;

  @JsonCreator
  public QuantilesPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probabilities") float[] probabilities
  )
  {
    super(name, fieldName);
    this.probabilities = probabilities;
    this.fieldName = fieldName;

    for (float p : probabilities) {
      if (p < 0 | p > 1) {
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
    final ApproximateHistogram ah = (ApproximateHistogram) values.get(this.getFieldName());

    return new Quantiles(this.getProbabilities(), ah.getQuantiles(this.getProbabilities()), ah.getMin(), ah.getMax());
  }

  @JsonProperty
  public float[] getProbabilities()
  {
    return probabilities;
  }

  @Override
  public String toString()
  {
    return "EqualBucketsPostAggregator{" +
           "name='" + this.getName() + '\'' +
           ", fieldName='" + this.getFieldName() + '\'' +
           ", probabilities=" + Arrays.toString(this.getProbabilities()) +
           '}';
  }
}
