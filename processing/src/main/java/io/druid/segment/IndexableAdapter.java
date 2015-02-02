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

package io.druid.segment;

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import org.joda.time.Interval;

/**
 * An adapter to an index
 */
public interface IndexableAdapter
{
  Interval getDataInterval();

  int getNumRows();

  Indexed<String> getDimensionNames();

  Indexed<String> getMetricNames();

  Indexed<String> getDimValueLookup(String dimension);

  Iterable<Rowboat> getRows();

  IndexedInts getBitmapIndex(String dimension, String value);

  String getMetricType(String metric);

  ColumnCapabilities getCapabilities(String column);
}
