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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.RangeSet;
import io.druid.query.Druids;
import io.druid.segment.filter.NotFilter;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class NotDimFilter implements DimFilter
{
  private static final Function<DimFilter, DimFilter> NEGATE =
      filter -> Druids.newNotDimFilterBuilder().field(filter).build();

  final private DimFilter field;

  @JsonCreator
  public NotDimFilter(
      @JsonProperty("field") DimFilter field
  )
  {
    Preconditions.checkArgument(field != null, "NOT operator requires at least one field");
    this.field = field;
  }

  @JsonProperty("field")
  public DimFilter getField()
  {
    return field;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] subKey = field.getCacheKey();

    return ByteBuffer.allocate(1 + subKey.length).put(DimFilterUtils.NOT_CACHE_ID).put(subKey).array();
  }

  @Override
  public DimFilter optimize()
  {
    return Druids.newNotDimFilterBuilder().field(this.getField().optimize()).build();
  }

  @Override
  public Filter toFilter()
  {
    return new NotFilter(field.toFilter());
  }

  /**
   * There are some special cases involving null that require special casing for And and Or instead of simply taking
   * the complement
   *
   * Example 1 : "NOT ( [0,INF) OR null)" The inside of NOT would evaluate to null, and the complement would also
   * be null. However, by breaking the NOT, this statement is "NOT([0,INF)) AND NOT(null)", which means it should
   * actually evaluate to (-INF, 0).
   *
   * Example 2 : "NOT ( [0,INF) AND null )" The inside of NOT would evaluate to [0,INF), and the complement would be
   * (-INF, 0). However the statement is actually "NOT([0,INF)) OR NOT(null)", and it should be evaluated to null.
   */
  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (field instanceof AndDimFilter) {
      List<DimFilter> fields = ((AndDimFilter) field).getFields();
      return Druids.newOrDimFilterBuilder().fields(Lists.transform(fields, NEGATE)).build().getDimensionRangeSet(dimension);
    }
    if (field instanceof OrDimFilter) {
      List<DimFilter> fields = ((OrDimFilter) field).getFields();
      return Druids.newAndDimFilterBuilder().fields(Lists.transform(fields, NEGATE)).build().getDimensionRangeSet(dimension);
    }
    if (field instanceof NotDimFilter) {
      return ((NotDimFilter) field).getField().getDimensionRangeSet(dimension);
    }
    RangeSet<String> rangeSet = field.getDimensionRangeSet(dimension);
    return rangeSet == null ? null : rangeSet.complement();
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

    NotDimFilter that = (NotDimFilter) o;

    if (field != null ? !field.equals(that.field) : that.field != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return field != null ? field.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "!" + field;
  }
}
