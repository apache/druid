/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.brita;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.metamx.druid.query.search.SearchQuerySpec;

import javax.annotation.Nullable;

/**
 */
public class SearchQueryFilter extends DimensionPredicateFilter
{
  @JsonCreator
  public SearchQueryFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("query") final SearchQuerySpec query
  )
  {
    super(
        dimension,
        new Predicate<String>()
        {
          @Override
          public boolean apply(@Nullable String input)
          {
            return query.accept(input);
          }
        }
    );
  }
}
