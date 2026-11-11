/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client.cache;

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 */
public class MapCacheTest extends CacheTestBase<MapCache>
{
  private static final byte[] HI = StringUtils.toUtf8("hi");
  private static final byte[] HO = StringUtils.toUtf8("ho");
  private ByteCountingLRUMap baseMap;

  @BeforeEach
  public void setUp()
  {
    baseMap = new ByteCountingLRUMap(1024 * 1024);
    cache = new MapCache(baseMap);
  }

  @Test
  public void testSanity()
  {
    Assertions.assertNull(cache.get(new Cache.NamedKey("a", HI)));
    Assertions.assertEquals(0, baseMap.size());
    put(cache, "a", HI, 1);
    Assertions.assertEquals(1, baseMap.size());
    Assertions.assertEquals(1, get(cache, "a", HI));
    Assertions.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    put(cache, "the", HI, 2);
    Assertions.assertEquals(2, baseMap.size());
    Assertions.assertEquals(1, get(cache, "a", HI));
    Assertions.assertEquals(2, get(cache, "the", HI));

    put(cache, "the", HO, 10);
    Assertions.assertEquals(3, baseMap.size());
    Assertions.assertEquals(1, get(cache, "a", HI));
    Assertions.assertNull(cache.get(new Cache.NamedKey("a", HO)));
    Assertions.assertEquals(2, get(cache, "the", HI));
    Assertions.assertEquals(10, get(cache, "the", HO));

    cache.close("the");
    Assertions.assertEquals(1, baseMap.size());
    Assertions.assertEquals(1, get(cache, "a", HI));
    Assertions.assertNull(cache.get(new Cache.NamedKey("a", HO)));

    cache.close("a");
    Assertions.assertEquals(0, baseMap.size());
  }

  public void put(Cache cache, String namespace, byte[] key, Integer value)
  {
    cache.put(new Cache.NamedKey(namespace, key), Ints.toByteArray(value));
  }

  public int get(Cache cache, String namespace, byte[] key)
  {
    return Ints.fromByteArray(cache.get(new Cache.NamedKey(namespace, key)));
  }
}
