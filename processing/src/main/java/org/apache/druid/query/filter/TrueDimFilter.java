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
import com.google.common.collect.RangeSet;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.filter.TrueFilter;

import java.util.Collections;
import java.util.Set;

public class TrueDimFilter implements DimFilter
{
  private static final TrueDimFilter INSTANCE = new TrueDimFilter();
  private static final byte[] CACHE_KEY = new CacheKeyBuilder(DimFilterUtils.TRUE_CACHE_ID).build();

  @JsonCreator
  public static TrueDimFilter instance()
  {
    return INSTANCE;
  }

  private TrueDimFilter()
  {
  }

  @Override
  public byte[] getCacheKey()
  {
    return CACHE_KEY;
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return TrueFilter.instance();
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Collections.emptySet();
  }

  @Override
  public int hashCode()
  {
    return DimFilterUtils.TRUE_CACHE_ID;
  }

  @Override
  public boolean equals(Object o)
  {
    return o != null && o.getClass() == this.getClass();
  }

  @Override
  public String toString()
  {
    return "TRUE";
  }
}
