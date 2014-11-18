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
