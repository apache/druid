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

package io.druid.query.aggregation.histogram;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;

public class BufferUtils
{
  public static int binarySearch(DoubleBuffer buf, int minIndex, int maxIndex, double value)
  {
    while (minIndex < maxIndex) {
      int currIndex = (minIndex + maxIndex - 1) >>> 1;

      double currValue = buf.get(currIndex);
      int comparison = Double.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex;
      }
    }

    return -(minIndex + 1);
  }

  public static int binarySearch(FloatBuffer buf, int minIndex, int maxIndex, float value)
  {
    while (minIndex < maxIndex) {
      int currIndex = (minIndex + maxIndex - 1) >>> 1;

      float currValue = buf.get(currIndex);
      int comparison = Float.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex;
      }
    }

    return -(minIndex + 1);
  }
}
