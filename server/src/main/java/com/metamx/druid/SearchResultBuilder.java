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

package com.metamx.druid;

import com.google.common.collect.Lists;
import com.metamx.druid.query.search.SearchHit;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import org.joda.time.DateTime;

/**
 */
public class SearchResultBuilder
{
  private final DateTime timestamp;
  private final Iterable<SearchHit> hits;

  public SearchResultBuilder(
      DateTime timestamp,
      Iterable<SearchHit> hits
  )
  {
    this.timestamp = timestamp;
    this.hits = hits;
  }

  public Result<SearchResultValue> build()
  {
    return new Result<SearchResultValue>(
        timestamp,
        new SearchResultValue(Lists.newArrayList(hits))
    );
  }
}
