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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.druid.query.ordering.StringComparators;

import java.util.Comparator;

/**
 */
public class AlphanumericSearchSortSpec implements SearchSortSpec
{
  @JsonCreator
  public AlphanumericSearchSortSpec(
  )
  {
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>()
    {
      @Override
      public int compare(SearchHit searchHit1, SearchHit searchHit2)
      {
        int retVal = StringComparators.ALPHANUMERIC.compare(
            searchHit1.getValue(), searchHit2.getValue());
        if (retVal == 0) {
          retVal = StringComparators.LEXICOGRAPHIC.compare(
              searchHit1.getDimension(), searchHit2.getDimension());
        }
        return retVal;
      }
    };
  }

  public String toString()
  {
    return "alphanumericSort";
  }

  @Override
  public boolean equals(Object other) {
    return this == other || other instanceof AlphanumericSearchSortSpec;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public byte[] getCacheKey()
  {
    return toString().getBytes();
  }
}
