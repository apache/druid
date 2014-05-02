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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.druid.query.search.search.SearchQuerySpec;

import java.nio.ByteBuffer;

/**
 */
public class SearchQueryDimFilter implements DimFilter
{
  private final String dimension;
  private final SearchQuerySpec query;

  public SearchQueryDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("query") SearchQuerySpec query
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(query != null, "query must not be null");

    this.dimension = dimension;
    this.query = query;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public SearchQuerySpec getQuery()
  {
    return query;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = dimension.getBytes(Charsets.UTF_8);
    final byte[] queryBytes = query.getCacheKey();

    return ByteBuffer.allocate(1 + dimensionBytes.length + queryBytes.length)
                     .put(DimFilterCacheHelper.SEARCH_QUERY_TYPE_ID)
                     .put(dimensionBytes)
                     .put(queryBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "SearchQueryDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", query=" + query +
           '}';
  }
}
