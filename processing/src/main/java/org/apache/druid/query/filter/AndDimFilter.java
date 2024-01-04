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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.filter.Filters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class AndDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private static final Joiner AND_JOINER = Joiner.on(" && ");

  private final List<DimFilter> fields;

  @JsonCreator
  public AndDimFilter(
      @JsonProperty("fields") List<DimFilter> fields
  )
  {
    fields = DimFilters.filterNulls(fields);
    Preconditions.checkArgument(fields.size() > 0, "AND operator requires at least one field");
    this.fields = fields;
  }

  public AndDimFilter(DimFilter... fields)
  {
    this(Arrays.asList(fields));
  }

  @JsonProperty
  public List<DimFilter> getFields()
  {
    return fields;
  }

  @Override
  public byte[] getCacheKey()
  {
    return DimFilterUtils.computeCacheKey(DimFilterUtils.AND_CACHE_ID, fields);
  }

  @Override
  public DimFilter optimize(final boolean mayIncludeUnknown)
  {
    // This method optimizes children, but doesn't do any special AND-related stuff like flattening or duplicate
    // removal. That will happen in "toFilter", which allows us to share code with Filters.and(...).

    final List<DimFilter> newFields = DimFilters.optimize(fields, mayIncludeUnknown);

    if (newFields.size() == 1) {
      return newFields.get(0);
    } else {
      return new AndDimFilter(newFields);
    }
  }

  @Override
  public Filter toFilter()
  {
    final List<Filter> filters = new ArrayList<>(fields.size());

    for (final DimFilter field : fields) {
      filters.add(field.toFilter());
    }

    return Filters.and(filters);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    RangeSet<String> retSet = null;
    for (DimFilter field : fields) {
      RangeSet<String> rangeSet = field.getDimensionRangeSet(dimension);
      if (rangeSet != null) {
        if (retSet == null) {
          retSet = TreeRangeSet.create(rangeSet);
        } else {
          retSet.removeAll(rangeSet.complement());
        }
      }
    }
    return retSet;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    HashSet<String> requiredColumns = new HashSet<>();
    fields.forEach(field -> requiredColumns.addAll(field.getRequiredColumns()));
    return requiredColumns;
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

    AndDimFilter that = (AndDimFilter) o;

    if (fields != null ? !fields.equals(that.fields) : that.fields != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return fields != null ? fields.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("(%s)", AND_JOINER.join(fields));
  }
}
