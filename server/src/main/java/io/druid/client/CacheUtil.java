/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.client.cache.Cache;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CacheUtil
{
  public static Cache.NamedKey computeSegmentCacheKey(
      String segmentIdentifier,
      SegmentDescriptor descriptor,
      byte[] queryCacheKey
  )
  {
    final Interval segmentQueryInterval = descriptor.getInterval();
    final byte[] versionBytes = io.druid.java.util.common.StringUtils.toUtf8(descriptor.getVersion());

    return new Cache.NamedKey(
        segmentIdentifier, ByteBuffer
        .allocate(16 + versionBytes.length + 4 + queryCacheKey.length)
        .putLong(segmentQueryInterval.getStartMillis())
        .putLong(segmentQueryInterval.getEndMillis())
        .put(versionBytes)
        .putInt(descriptor.getPartitionNumber())
        .put(queryCacheKey).array()
    );
  }

  public static void populate(Cache cache, ObjectMapper mapper, Cache.NamedKey key, Iterable<Object> results)
  {
    try {
      List<byte[]> bytes = Lists.newArrayList();
      int size = 0;
      for (Object result : results) {
        final byte[] array = mapper.writeValueAsBytes(result);
        size += array.length;
        bytes.add(array);
      }

      byte[] valueBytes = new byte[size];
      int offset = 0;
      for (byte[] array : bytes) {
        System.arraycopy(array, 0, valueBytes, offset, array.length);
        offset += array.length;
      }

      cache.put(key, valueBytes);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

}
