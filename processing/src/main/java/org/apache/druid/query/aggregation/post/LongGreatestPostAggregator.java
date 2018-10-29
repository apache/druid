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
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.Queries;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class LongGreatestPostAggregator implements PostAggregator
{
  private static final Comparator<Number> COMPARATOR = Comparator.nullsFirst(
      Comparator.comparingLong(Number::longValue)
  );

  private final String name;
  private final List<PostAggregator> fields;

  @JsonCreator
  public LongGreatestPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fields") List<PostAggregator> fields
  )
  {
    Preconditions.checkArgument(fields != null && fields.size() > 0, "Illegal number of fields[%s], must be > 0");

    this.name = name;
    this.fields = fields;
  }

  @Override
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = new HashSet<>();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    Iterator<PostAggregator> fieldsIter = fields.iterator();
    Long retVal = NullHandling.replaceWithDefault() ? Long.MIN_VALUE : null;
    while (fieldsIter.hasNext()) {
      Number nextVal = ((Number) fieldsIter.next().compute(values));
      // Ignore NULL values and return the greatest out of non-null values.
      if (nextVal != null && (retVal == null || COMPARATOR.compare(nextVal, retVal) > 0)) {
        if (nextVal instanceof Long) {
          retVal = (Long) nextVal;
        } else {
          retVal = nextVal.longValue();
        }
      }
    }
    return retVal;
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public LongGreatestPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return new LongGreatestPostAggregator(name, Queries.decoratePostAggregators(fields, aggregators));
  }

  @JsonProperty
  public List<PostAggregator> getFields()
  {
    return fields;
  }

  @Override
  public String toString()
  {
    return "LongGreatestPostAggregator{" +
           "name='" + name + '\'' +
           ", fields=" + fields +
           "}";
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

    LongGreatestPostAggregator that = (LongGreatestPostAggregator) o;

    if (!fields.equals(that.fields)) {
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
    result = 31 * result + fields.hashCode();
    return result;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(PostAggregatorIds.LONG_GREATEST)
        .appendCacheablesIgnoringOrder(fields)
        .build();
  }
}
