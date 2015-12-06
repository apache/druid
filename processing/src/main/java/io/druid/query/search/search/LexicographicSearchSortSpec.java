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

import java.util.Comparator;

/**
 */
public class LexicographicSearchSortSpec implements SearchSortSpec
{
  @JsonCreator
  public LexicographicSearchSortSpec(
  )
  {
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>()
    {
      @Override
      public int compare(SearchHit searchHit, SearchHit searchHit1)
      {
        int retVal = searchHit.getValue().compareTo(searchHit1.getValue());
        if (retVal == 0) {
          retVal = searchHit.getDimension().compareTo(searchHit1.getDimension());
        }
        return retVal;
      }
    };
  }

  public String toString()
  {
    return "lexicographicSort";
  }

  @Override
  public boolean equals(Object other) {
    return (other instanceof LexicographicSearchSortSpec);
  }
}
