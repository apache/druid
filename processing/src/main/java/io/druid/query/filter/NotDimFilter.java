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
import com.google.common.base.Preconditions;
import io.druid.query.Druids;
import io.druid.segment.filter.NotFilter;

import java.nio.ByteBuffer;

/**
 */
public class NotDimFilter implements DimFilter
{
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

    return ByteBuffer.allocate(1 + subKey.length).put(DimFilterCacheHelper.NOT_CACHE_ID).put(subKey).array();
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
