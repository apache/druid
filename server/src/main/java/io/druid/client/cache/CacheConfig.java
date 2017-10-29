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
  @Min(0)
  private int cacheBulkMergeLimit = Integer.MAX_VALUE;

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

  public int getNumBackgroundThreads()
  {
    return numBackgroundThreads;
  }

  public int getCacheBulkMergeLimit()
  {
    return cacheBulkMergeLimit;
  }

  public boolean isQueryCacheable(Query query)
  {
    return isQueryCacheable(query.getType());
  }

  public boolean isQueryCacheable(String queryType)
  {
    // O(n) impl, but I don't think we'll ever have a million query types here
    return !unCacheable.contains(queryType);
  }
}
