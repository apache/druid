package com.metamx.druid.client.cache;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
