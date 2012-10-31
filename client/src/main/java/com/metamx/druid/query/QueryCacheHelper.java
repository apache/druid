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

package com.metamx.druid.query;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import com.metamx.druid.aggregation.AggregatorFactory;

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
