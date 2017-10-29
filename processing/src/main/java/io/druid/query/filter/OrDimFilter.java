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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.druid.java.util.common.StringUtils;
import io.druid.query.Druids;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.OrFilter;

import java.util.List;

/**
 */
public class OrDimFilter implements DimFilter
{
  private static final Joiner OR_JOINER = Joiner.on(" || ");

  final private List<DimFilter> fields;

  @JsonCreator
  public OrDimFilter(
      @JsonProperty("fields") List<DimFilter> fields
  )
  {
    fields = DimFilters.filterNulls(fields);
    Preconditions.checkArgument(fields.size() > 0, "OR operator requires at least one field");
    this.fields = fields;
  }

  @JsonProperty
  public List<DimFilter> getFields()
  {
    return fields;
  }

  @Override
  public byte[] getCacheKey()
  {
    return DimFilterUtils.computeCacheKey(DimFilterUtils.OR_CACHE_ID, fields);
  }

  @Override
  public DimFilter optimize()
  {
    List<DimFilter> elements = DimFilters.optimize(fields);
    return elements.size() == 1 ? elements.get(0) : Druids.newOrDimFilterBuilder().fields(elements).build();
  }

  @Override
  public Filter toFilter()
  {
    return new OrFilter(Filters.toFilters(fields));
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    RangeSet<String> retSet = TreeRangeSet.create();
    for (DimFilter field : fields) {
      RangeSet<String> rangeSet = field.getDimensionRangeSet(dimension);
      if (rangeSet == null) {
        return null;
      } else {
        retSet.addAll(rangeSet);
      }
    }
    return retSet;
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

    OrDimFilter that = (OrDimFilter) o;

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
    return StringUtils.format("(%s)", OR_JOINER.join(fields));
  }
}
