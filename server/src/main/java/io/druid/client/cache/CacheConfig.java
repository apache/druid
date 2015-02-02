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

package io.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.Query;

import javax.validation.constraints.Min;
import java.util.Arrays;
import java.util.List;

public class CacheConfig
{
  public static final String USE_CACHE = "useCache";
  public static final String POPULATE_CACHE = "populateCache";

  @JsonProperty
  private boolean useCache = false;

  @JsonProperty
  private boolean populateCache = false;

  @JsonProperty
  @Min(0)
  private int numBackgroundThreads = 0;

  @JsonProperty
  private List<String> unCacheable = Arrays.asList(Query.GROUP_BY, Query.SELECT);

  public boolean isPopulateCache()
  {
    return populateCache;
  }

  public boolean isUseCache()
  {
    return useCache;
  }

  public int getNumBackgroundThreads(){
    return numBackgroundThreads;
  }

  public boolean isQueryCacheable(Query query)
  {
    // O(n) impl, but I don't think we'll ever have a million query types here
    return !unCacheable.contains(query.getType());
  }
}
