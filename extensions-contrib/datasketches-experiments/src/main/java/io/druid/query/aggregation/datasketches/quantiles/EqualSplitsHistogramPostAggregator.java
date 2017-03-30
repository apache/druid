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

package io.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.yahoo.sketches.quantiles.DoublesSketch;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class EqualSplitsHistogramPostAggregator implements PostAggregator
{

  private final String name;
  private final int numSplits;
  private final String fieldName;

  @JsonCreator
  public EqualSplitsHistogramPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("numSplits") int numSplits,
      @JsonProperty("fieldName") String fieldName

  )
  {
    this.name = Preconditions.checkNotNull(name, "name is null");
    Preconditions.checkArgument(numSplits > 1, "numSplits must be > 1");
    this.numSplits = numSplits;
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.of(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    DoublesSketch sketch = ((DoublesSketchHolder) combinedAggregators.get(fieldName)).getSketch();

    double min = sketch.getMinValue();
    double max = sketch.getMaxValue();

    if (max > min) {
      double splitWidth = (max - min) / numSplits;
      double[] splits = new double[numSplits - 1];

      for (int i = 0; i < splits.length; i++) {
        splits[i] = min + (i + 1) * splitWidth;
      }

      long n = sketch.getN();
      double[] result = sketch.getPMF(splits);
      for (int i = 0; i < result.length; i++) {
        result[i] *= n;
      }
      return result;
    } else {
      //all values in the sketch are same, just put all in one bucket
      //and remaining 0s
      double[] result = new double[numSplits];
      result[0] = sketch.getN();
      return result;
    }
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getNumSplits()
  {
    return numSplits;
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

    EqualSplitsHistogramPostAggregator that = (EqualSplitsHistogramPostAggregator) o;

    if (numSplits != that.numSplits) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return fieldName.equals(that.fieldName);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + numSplits;
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "EqualSplitsHistogramPostAggregator{" +
           "name='" + name + '\'' +
           ", numSplits=" + numSplits +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
