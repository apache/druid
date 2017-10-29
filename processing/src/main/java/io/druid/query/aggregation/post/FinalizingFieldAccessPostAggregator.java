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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class FinalizingFieldAccessPostAggregator implements PostAggregator
{
  private final String name;
  private final String fieldName;

  @JsonCreator
  public FinalizingFieldAccessPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    throw new UnsupportedOperationException("No decorated");
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public FinalizingFieldAccessPostAggregator decorate(final Map<String, AggregatorFactory> aggregators)
  {
    return new FinalizingFieldAccessPostAggregator(name, fieldName)
    {
      @Override
      public Comparator getComparator()
      {
        if (aggregators != null && aggregators.containsKey(fieldName)) {
          return aggregators.get(fieldName).getComparator();
        } else {
          return Comparators.naturalNullsFirst();
        }
      }

      @Override
      public Object compute(Map<String, Object> combinedAggregators)
      {
        if (aggregators != null && aggregators.containsKey(fieldName)) {
          return aggregators.get(fieldName).finalizeComputation(
              combinedAggregators.get(fieldName)
          );
        } else {
          return combinedAggregators.get(fieldName);
        }
      }
    };
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.FINALIZING_FIELD_ACCESS)
        .appendString(fieldName)
        .build();
  }

  @Override
  public String toString()
  {
    return "FinalizingFieldAccessPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
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

    FinalizingFieldAccessPostAggregator that = (FinalizingFieldAccessPostAggregator) o;

    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    return result;
  }

  @VisibleForTesting
  static FinalizingFieldAccessPostAggregator buildDecorated(
      String name,
      String fieldName,
      Map<String, AggregatorFactory> aggregators
  )
  {
    FinalizingFieldAccessPostAggregator ret = new FinalizingFieldAccessPostAggregator(name, fieldName);
    return ret.decorate(aggregators);
  }
}
