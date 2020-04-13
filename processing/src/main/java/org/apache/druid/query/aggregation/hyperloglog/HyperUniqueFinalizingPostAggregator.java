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

package org.apache.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.PostAggregatorIds;
import org.apache.druid.query.cache.CacheKeyBuilder;

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
  private final AggregatorFactory aggregatorFactory;

  @JsonCreator
  public HyperUniqueFinalizingPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this(name, fieldName, null);
  }

  private HyperUniqueFinalizingPostAggregator(
      String name,
      String fieldName,
      AggregatorFactory aggregatorFactory
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
    //Note that, in general, name shouldn't be null, we are defaulting
    //to fieldName here just to be backward compatible with 0.7.x
    this.name = name == null ? fieldName : name;
    this.aggregatorFactory = aggregatorFactory;
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
    final Object collector = combinedAggregators.get(fieldName);

    if (aggregatorFactory == null) {
      // This didn't come directly from an aggregator. Maybe it came through a FieldAccessPostAggregator or
      // something like that. Hope it's a HyperLogLogCollector, and estimate it without rounding.
      return HyperUniquesAggregatorFactory.estimateCardinality(collector, false);
    } else {
      // Delegate to the aggregator factory to get the user-specified rounding behavior.
      return aggregatorFactory.finalizeComputation(collector);
    }
  }

  @Override
  @JsonProperty("name")
  public String getName()
  {
    return name;
  }

  @Override
  public HyperUniqueFinalizingPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    final AggregatorFactory theAggregatorFactory = aggregators != null ? aggregators.get(fieldName) : null;
    return new HyperUniqueFinalizingPostAggregator(name, fieldName, theAggregatorFactory);
  }

  @JsonProperty("fieldName")
  public String getFieldName()
  {
    return fieldName;
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

    HyperUniqueFinalizingPostAggregator that = (HyperUniqueFinalizingPostAggregator) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    return fieldName != null ? fieldName.equals(that.fieldName) : that.fieldName == null;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "HyperUniqueFinalizingPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.HLL_HYPER_UNIQUE_FINALIZING)
        .appendString(fieldName)
        .build();
  }
}
