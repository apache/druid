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

package org.apache.druid.query.aggregation.post;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class FinalizingFieldAccessPostAggregator implements PostAggregator
{
  private final String name;
  private final String fieldName;
  private final Comparator<Object> comparator;
  private final Function<Object, Object> finalizer;

  @JsonCreator
  public FinalizingFieldAccessPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this(name, fieldName, null, null);
  }

  private FinalizingFieldAccessPostAggregator(
      final String name,
      final String fieldName,
      final Comparator<Object> comparator,
      final Function<Object, Object> finalizer
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.comparator = comparator;
    this.finalizer = finalizer;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    if (comparator == null) {
      throw new UnsupportedOperationException("Not decorated");
    } else {
      return comparator;
    }
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    if (finalizer == null) {
      throw new UnsupportedOperationException("Not decorated");
    } else {
      return finalizer.apply(combinedAggregators.get(fieldName));
    }
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
    final Comparator<Object> theComparator;
    final Function<Object, Object> theFinalizer;

    if (aggregators != null && aggregators.containsKey(fieldName)) {
      //noinspection unchecked
      theComparator = aggregators.get(fieldName).getComparator();
    } else {
      //noinspection unchecked
      theComparator = (Comparator) Comparators.naturalNullsFirst();
    }

    if (aggregators != null && aggregators.containsKey(fieldName)) {
      theFinalizer = aggregators.get(fieldName)::finalizeComputation;
    } else {
      theFinalizer = Function.identity();
    }

    return new FinalizingFieldAccessPostAggregator(
        name,
        fieldName,
        theComparator,
        theFinalizer
    );
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
}
