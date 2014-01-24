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

package io.druid.query.select;

import com.metamx.common.guava.nary.BinaryFn;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Result;
import org.joda.time.DateTime;

/**
 */
public class SelectBinaryFn
    implements BinaryFn<Result<SelectResultValue>, Result<SelectResultValue>, Result<SelectResultValue>>
{
  private final QueryGranularity gran;
  private final PagingSpec pagingSpec;

  public SelectBinaryFn(
      QueryGranularity granularity,
      PagingSpec pagingSpec
  )
  {
    this.gran = granularity;
    this.pagingSpec = pagingSpec;
  }

  @Override
  public Result<SelectResultValue> apply(
      Result<SelectResultValue> arg1, Result<SelectResultValue> arg2
  )
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    final DateTime timestamp = (gran instanceof AllGranularity)
                               ? arg1.getTimestamp()
                               : gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis()));

    SelectResultValueBuilder builder = new SelectResultValueBuilder(timestamp, pagingSpec.getThreshold());

    SelectResultValue arg1Val = arg1.getValue();
    SelectResultValue arg2Val = arg2.getValue();

    for (EventHolder event : arg1Val) {
      builder.addEntry(event);
    }

    for (EventHolder event : arg2Val) {
      builder.addEntry(event);
    }

    return builder.build();
  }
}
