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

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

/**
 */
public interface TopNAlgorithm<DimValSelector, Parameters extends TopNParams>
{
  public static final Aggregator[] EMPTY_ARRAY = {};
  public static final int INIT_POSITION_VALUE = -1;
  public static final int SKIP_POSITION_VALUE = -2;

  public TopNParams makeInitParams(DimensionSelector dimSelector, Cursor cursor);

  public void run(
      Parameters params,
      TopNResultBuilder resultBuilder,
      DimValSelector dimValSelector
  );

  public void cleanup(Parameters params);
}
