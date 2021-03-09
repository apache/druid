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
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.com.google.common.collect.Sets;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
public class FieldAccessPostAggregator implements PostAggregator
{
  @Nullable
  private final String name;
  private final String fieldName;
  // type is ignored from equals and friends because it is computed by decorate, and all post-aggs should be decorated
  // prior to usage (and is currently done so in the query constructors of all queries which can have post-aggs)
  @Nullable
  private final ValueType type;

  @JsonCreator
  public FieldAccessPostAggregator(
      @JsonProperty("name") @Nullable String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this(name, fieldName, null);
  }

  private FieldAccessPostAggregator(@Nullable String name, String fieldName, @Nullable ValueType type)
  {
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
    this.type = type;
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
    return combinedAggregators.get(fieldName);
  }

  @Nullable
  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ValueType getType()
  {
    return type;
  }

  @Override
  public FieldAccessPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    final ValueType type;

    if (aggregators != null && aggregators.containsKey(fieldName)) {
      type = aggregators.get(fieldName).getType();
    } else {
      type = null;
    }

    return new FieldAccessPostAggregator(
        name,
        fieldName,
        type
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.FIELD_ACCESS)
        .appendString(fieldName)
        .build();
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String toString()
  {
    return "FieldAccessPostAggregator{" +
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

    FieldAccessPostAggregator that = (FieldAccessPostAggregator) o;

    if (!fieldName.equals(that.fieldName)) {
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
    result = 31 * result + fieldName.hashCode();
    return result;
  }
}
