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

import java.util.Comparator;

import com.google.common.primitives.Longs;
import com.metamx.druid.result.Result;

/**
 */
public class ResultGranularTimestampComparator<T> implements Comparator<Result<T>>
{
  private final QueryGranularity gran;

  public ResultGranularTimestampComparator(QueryGranularity granularity)
  {
    this.gran = granularity;
  }

  @Override
  public int compare(Result<T> r1, Result<T> r2)
  {
    return Longs.compare(
        gran.truncate(r1.getTimestamp().getMillis()),
        gran.truncate(r2.getTimestamp().getMillis())
    );
  }
}
