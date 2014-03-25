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

package io.druid.client.cache;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class MapCacheTest
{
  private static final byte[] HI = "hi".getBytes();
  private static final byte[] HO = "ho".getBytes();
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
