/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;

/**
 */
public class SpatialDimensionSchema
{
  private final String dimName;
  private final List<String> dims;

  @JsonCreator
  public SpatialDimensionSchema(
      @JsonProperty("dimName") String dimName,
      @JsonProperty("dims") List<String> dims
  )
  {
    this.dimName = dimName.toLowerCase();
    this.dims = Lists.transform(
        dims,
        new Function<String, String>()
        {
          @Override
          public String apply(String input)
          {
            return input.toLowerCase();
          }
        }
    );
  }

  @JsonProperty
  public String getDimName()
  {
    return dimName;
  }

  @JsonProperty
  public List<String> getDims()
  {
    return dims;
  }
}
