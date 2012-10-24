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

package com.metamx.druid.input;

import java.util.List;

/**
 */
public class Rows
{
  public static InputRow toInputRow(final Row row, final List<String> dimensions)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return dimensions;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return row.getTimestampFromEpoch();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return row.getDimension(dimension);
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return row.getFloatMetric(metric);
      }
    };
  }
}
