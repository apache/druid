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

package io.druid.query.filter;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
class DimFilterCacheHelper
{
  static final byte NOOP_CACHE_ID = -0x4;
  static final byte SELECTOR_CACHE_ID = 0x0;
  static final byte AND_CACHE_ID = 0x1;
  static final byte OR_CACHE_ID = 0x2;
  static final byte NOT_CACHE_ID = 0x3;
  static final byte EXTRACTION_CACHE_ID = 0x4;
  static final byte REGEX_CACHE_ID = 0x5;
  static final byte SEARCH_QUERY_TYPE_ID = 0x6;
  static final byte JAVASCRIPT_CACHE_ID = 0x7;
  static final byte SPATIAL_CACHE_ID = 0x8;

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
