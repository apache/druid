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

package io.druid.testing.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.Query;

import java.util.List;
import java.util.Map;

public class QueryWithResults
{
  private final Query query;
  private final List<Map<String, Object>> expectedResults;

  @JsonCreator
  public QueryWithResults(
      @JsonProperty("query") Query query,
      @JsonProperty("expectedResults") List<Map<String, Object>> expectedResults
  )
  {
    this.query = query;
    this.expectedResults = expectedResults;
  }

  @JsonProperty
  public Query getQuery()
  {
    return query;
  }

  @JsonProperty
  public List<Map<String, Object>> getExpectedResults()
  {
    return expectedResults;
  }

  @Override
  public String toString()
  {
    return "QueryWithResults{" +
           "query=" + query +
           ", expectedResults=" + expectedResults +
           '}';
  }
}