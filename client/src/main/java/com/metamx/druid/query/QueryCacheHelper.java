package com.metamx.druid.query;

import com.google.common.collect.Lists;
import com.metamx.druid.aggregation.AggregatorFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class QueryCacheHelper
{
  public static byte[] computeAggregatorBytes(List<AggregatorFactory> aggregatorSpecs)
  {
    List<byte[]> cacheKeySet = Lists.newArrayListWithCapacity(aggregatorSpecs.size());

    int totalSize = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      final byte[] cacheKey = spec.getCacheKey();
      cacheKeySet.add(cacheKey);
      totalSize += cacheKey.length;
    }

    ByteBuffer retVal = ByteBuffer.allocate(totalSize);
    for (byte[] bytes : cacheKeySet) {
      retVal.put(bytes);
    }
    return retVal.array();
  }

}
