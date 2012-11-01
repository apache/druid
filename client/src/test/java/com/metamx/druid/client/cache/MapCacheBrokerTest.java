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

package com.metamx.druid.client.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.Ints;

/**
 */
public class MapCacheBrokerTest
{
  private static final byte[] HI = "hi".getBytes();
  private static final byte[] HO = "ho".getBytes();
  private ByteCountingLRUMap baseMap;
  private MapCacheBroker broker;

  @Before
  public void setUp() throws Exception
  {
    baseMap = new ByteCountingLRUMap(1024 * 1024);
    broker = new MapCacheBroker(baseMap);
  }

  @Test
  public void testSanity() throws Exception
  {
    Cache aCache = broker.provideCache("a");
    Cache theCache = broker.provideCache("the");

    Assert.assertNull(aCache.get(HI));
    Assert.assertEquals(0, baseMap.size());
    put(aCache, HI, 1);
    Assert.assertEquals(1, baseMap.size());
    Assert.assertEquals(1, get(aCache, HI));
    Assert.assertNull(theCache.get(HI));

    put(theCache, HI, 2);
    Assert.assertEquals(2, baseMap.size());
    Assert.assertEquals(1, get(aCache, HI));
    Assert.assertEquals(2, get(theCache, HI));

    put(theCache, HO, 10);
    Assert.assertEquals(3, baseMap.size());
    Assert.assertEquals(1, get(aCache, HI));
    Assert.assertNull(aCache.get(HO));
    Assert.assertEquals(2, get(theCache, HI));
    Assert.assertEquals(10, get(theCache, HO));

    theCache.close();
    Assert.assertEquals(1, baseMap.size());
    Assert.assertEquals(1, get(aCache, HI));
    Assert.assertNull(aCache.get(HO));

    aCache.close();
    Assert.assertEquals(0, baseMap.size());
  }

  public void put(Cache cache, byte[] key, Integer value)
  {
    cache.put(key, Ints.toByteArray(value));
  }

  public int get(Cache cache, byte[] key)
  {
    return Ints.fromByteArray(cache.get(key));
  }
}
