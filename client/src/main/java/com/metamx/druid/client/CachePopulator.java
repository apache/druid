package com.metamx.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.druid.client.cache.Cache;

import java.io.IOException;
import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: himadri
* Date: 10/30/13
* Time: 1:40 PM
* To change this template use File | Settings | File Templates.
*/
public class CachePopulator
{
  private final Cache cache;
  private final ObjectMapper mapper;
  private final Cache.NamedKey key;

  public CachePopulator(Cache cache, ObjectMapper mapper, Cache.NamedKey key)
  {
    this.cache = cache;
    this.mapper = mapper;
    this.key = key;
  }

  public void populate(Iterable<Object> results)
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
