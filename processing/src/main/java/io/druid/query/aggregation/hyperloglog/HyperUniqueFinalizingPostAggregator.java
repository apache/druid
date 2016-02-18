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

package io.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class HyperUniqueFinalizingPostAggregator implements PostAggregator
{
  private static final Comparator<Double> DOUBLE_COMPARATOR = Ordering.from(new Comparator<Double>()
  {
    @Override
    public int compare(Double lhs, Double rhs)
    {
      return Double.compare(lhs, rhs);
    }
  }).nullsFirst();

  private final String name;
  private final String fieldName;

  @JsonCreator
  public HyperUniqueFinalizingPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
    //Note that, in general, name shouldn't be null, we are defaulting
    //to fieldName here just to be backward compatible with 0.7.x
    this.name = name == null ? fieldName : name;

  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return DOUBLE_COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return HyperUniquesAggregatorFactory.estimateCardinality(combinedAggregators.get(fieldName));
  }

  @Override
  @JsonProperty("name")
  public String getName()
  {
    return name;
  }

  @JsonProperty("fieldName")
  public String getFieldName()
  {
    return fieldName;
  }
}
