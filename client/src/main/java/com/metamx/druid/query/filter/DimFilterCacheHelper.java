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

package com.metamx.druid.query.filter;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
class DimFilterCacheHelper
{
  static byte[] computeCacheKey(byte cacheIdKey, List<DimFilter> filters)
  {
    if (filters.size() == 1) {
      return filters.get(0).getCacheKey();
    }

    byte[][] cacheKeys = new byte[filters.size()][];
    int totalSize = 0;
    int index = 0;
    for (DimFilter field : filters) {
      cacheKeys[index] = field.getCacheKey();
      totalSize += cacheKeys[index].length;
      ++index;
    }

    ByteBuffer retVal = ByteBuffer.allocate(1 + totalSize);
    retVal.put(cacheIdKey);
    for (byte[] cacheKey : cacheKeys) {
      retVal.put(cacheKey);
    }
    return retVal.array();
  }
}
