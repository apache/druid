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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.StringUtils;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;

import java.util.Comparator;

public class SearchSortSpec
{
  public static final StringComparator DEFAULT_ORDERING = StringComparators.LEXICOGRAPHIC;

  private final StringComparator ordering;

  @JsonCreator
  public SearchSortSpec(
      @JsonProperty("type") StringComparator ordering
  )
  {
    this.ordering = ordering == null ? DEFAULT_ORDERING : ordering;
  }

  @JsonProperty("type")
  public StringComparator getOrdering()
  {
    return ordering;
  }

  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>()
    {
      @Override
      public int compare(SearchHit searchHit, SearchHit searchHit1)
      {
        int retVal = ordering.compare(
            searchHit.getValue(), searchHit1.getValue());

        if (retVal == 0) {
          retVal = StringComparators.LEXICOGRAPHIC.compare(
              searchHit.getDimension(), searchHit1.getDimension());
        }
        return retVal;
      }
    };
  }

  public byte[] getCacheKey()
  {
    return ordering.getCacheKey();
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%sSort", ordering.toString());
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

    SearchSortSpec that = (SearchSortSpec) o;

    return ordering.equals(that.ordering);

  }

  @Override
  public int hashCode()
  {
    return ordering.hashCode();
  }
}
