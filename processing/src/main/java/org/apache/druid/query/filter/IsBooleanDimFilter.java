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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.RangeSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.filter.IsBooleanFilter;

import java.util.Objects;
import java.util.Set;

/**
 * Abstract SQL three-value logic wrapper for some child {@link DimFilter} to implement '{filter} IS TRUE' and
 * '{filter} IS FALSE'.
 *
 * @see IsTrueDimFilter   - IS TRUE
 * @see IsFalseDimFilter  - IS FALSE
 * @see IsBooleanFilter   - actual filtering logic
 */
public abstract class IsBooleanDimFilter extends AbstractOptimizableDimFilter
{
  private final DimFilter field;
  private final boolean isTrue;

  public IsBooleanDimFilter(
      DimFilter field,
      boolean isTrue
  )
  {
    if (field == null) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("IS %s operator requires a non-null filter for field", isTrue ? "TRUE" : "FALSE");
    }
    this.field = field;
    this.isTrue = isTrue;
  }

  @JsonProperty("field")
  public DimFilter getField()
  {
    return field;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.IS_FILTER_BOOLEAN_FILTER_CACHE_ID).appendBoolean(isTrue)
                                                                                .appendCacheable(field)
                                                                                .build();
  }

  @Override
  public Filter toFilter()
  {
    return new IsBooleanFilter(field.toFilter(), isTrue);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return field.getRequiredColumns();
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

    IsBooleanDimFilter that = (IsBooleanDimFilter) o;

    if (field != null ? !field.equals(that.field) : that.field != null) {
      return false;
    }

    return isTrue == that.isTrue;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(field, isTrue);
  }

  @Override
  public String toString()
  {
    return "(" + field + ") IS " + (isTrue ? "TRUE" : "FALSE");
  }
}
