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
