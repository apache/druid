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

package io.druid.query.topn;

import com.google.common.base.Function;
import io.druid.query.Result;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import java.util.Comparator;

public class TopNMapFn implements Function<Cursor, Result<TopNResultValue>>
{
  private final TopNQuery query;
  private final TopNAlgorithm topNAlgorithm;


  public TopNMapFn(
      TopNQuery query,
      TopNAlgorithm topNAlgorithm
  )
  {
    this.query = query;
    this.topNAlgorithm = topNAlgorithm;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Result<TopNResultValue> apply(Cursor cursor)
  {
    final DimensionSelector dimSelector = cursor.makeDimensionSelector(query.getDimensionSpec().getDimension());
    if (dimSelector == null) {
      return null;
    }

    TopNParams params = null;
    try {
      params = topNAlgorithm.makeInitParams(dimSelector, cursor);

      TopNResultBuilder resultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, query);

      topNAlgorithm.run(params, resultBuilder, null);

      return resultBuilder.build();
    }
    finally {
      topNAlgorithm.cleanup(params);
    }
  }
}
