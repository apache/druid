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

package io.druid.client.cache;

import com.google.common.primitives.Ints;
import io.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class MapCacheTest
{
  private static final byte[] HI = StringUtils.toUtf8("hi");
  private static final byte[] HO = StringUtils.toUtf8("ho");
  private ByteCountingLRUMap baseMap;
  private MapCache cache;

  @Before
  public void setUp() throws Exception
  {
    baseMap = new ByteCountingLRUMap(1024 * 1024);
    cache = new MapCache(baseMap);
  }

  @Test
  public void testSanity() throws Exception
  {
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HI)));
    Assert.assertEquals(0, baseMap.size());
    put(cache, "a", HI, 1);
    Assert.assertEquals(1, baseMap.size());
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("the", HI)));

    put(cache, "the", HI, 2);
    Assert.assertEquals(2, baseMap.size());
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertEquals(2, get(cache, "the", HI));

    put(cache, "the", HO, 10);
    Assert.assertEquals(3, baseMap.size());
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));
    Assert.assertEquals(2, get(cache, "the", HI));
    Assert.assertEquals(10, get(cache, "the", HO));

    cache.close("the");
    Assert.assertEquals(1, baseMap.size());
    Assert.assertEquals(1, get(cache, "a", HI));
    Assert.assertNull(cache.get(new Cache.NamedKey("a", HO)));

    cache.close("a");
    Assert.assertEquals(0, baseMap.size());
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
