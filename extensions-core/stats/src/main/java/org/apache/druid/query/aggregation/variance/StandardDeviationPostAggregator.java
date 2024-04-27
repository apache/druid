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

package org.apache.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("stddev")
public class StandardDeviationPostAggregator implements PostAggregator
{
  protected final String name;
  protected final String fieldName;
  protected final String estimator;

  protected final boolean isVariancePop;

  @JsonCreator
  public StandardDeviationPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("estimator") String estimator
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
  }

  @Override
  @Nullable
  public Double compute(Map<String, Object> combinedAggregators)
  {
    Object varianceAggregatorCollector = combinedAggregators.get(fieldName);
    if (!(varianceAggregatorCollector instanceof VarianceAggregatorCollector)) {
      return NullHandling.defaultDoubleValue();
    }
    Double variance = ((VarianceAggregatorCollector) varianceAggregatorCollector).getVariance(isVariancePop);
    return variance == null ? NullHandling.defaultDoubleValue() : (Double) Math.sqrt(variance);
  }

  @Override
  @JsonProperty("name")
  public String getName()
  {
    return name;
  }

  @Override
  public ColumnType getType(ColumnInspector signature)
  {
    return ColumnType.DOUBLE;
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return this;
  }

  @JsonProperty("fieldName")
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty("estimator")
  public String getEstimator()
  {
    return estimator;
  }

  @Override
  public String toString()
  {
    return "StandardDeviationPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", estimator='" + estimator + '\'' +
           ", isVariancePop=" + isVariancePop +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.VARIANCE_STANDARD_DEVIATION)
        .appendString(fieldName)
        .appendString(estimator)
        .appendBoolean(isVariancePop)
        .build();
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

    StandardDeviationPostAggregator that = (StandardDeviationPostAggregator) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(estimator, that.estimator)) {
      return false;
    }
    if (!Objects.equals(isVariancePop, that.isVariancePop)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, estimator, isVariancePop);
  }
}
