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

import com.google.common.primitives.Ints;

import java.util.Comparator;

/**
 */
public class StrlenSearchSortSpec implements SearchSortSpec
{
  public StrlenSearchSortSpec()
  {
  }

  @Override
  public Comparator<SearchHit> getComparator()
  {
    return new Comparator<SearchHit>() {
      @Override
      public int compare(SearchHit s, SearchHit s1)
      {
        final String v1 = s.getValue();
        final String v2 = s1.getValue();
        int res = Ints.compare(v1.length(), v2.length());
        if (res == 0) {
          res = v1.compareTo(v2);
        }
        if (res == 0) {
          res = s.getDimension().compareTo(s1.getDimension());
        }
        return res;
      }
    };
  }

  public String toString()
  {
    return "stringLengthSort";
  }
}
