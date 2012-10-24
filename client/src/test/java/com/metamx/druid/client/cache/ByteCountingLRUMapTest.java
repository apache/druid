package com.metamx.druid.client.cache;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 */
public class ByteCountingLRUMapTest
{
  private ByteCountingLRUMap map;

  @Before
  public void setUp() throws Exception
  {
    map = new ByteCountingLRUMap(100);
  }

  @Test
  public void testSanity() throws Exception
  {
    final ByteBuffer tenKey = ByteBuffer.allocate(10);
    final byte[] eightyEightVal = ByteBuffer.allocate(88).array();
    final byte[] eightNineNineVal = ByteBuffer.allocate(89).array();
    final ByteBuffer oneByte = ByteBuffer.allocate(1);
    final ByteBuffer twoByte = ByteBuffer.allocate(2);

    assertMapValues(0, 0, 0);
    map.put(tenKey, eightNineNineVal);
    assertMapValues(1, 99, 0);
    Assert.assertEquals(ByteBuffer.wrap(eightNineNineVal), ByteBuffer.wrap(map.get(tenKey)));

    map.put(oneByte, oneByte.array());
    assertMapValues(1, 2, 1);
    Assert.assertNull(map.get(tenKey));
    Assert.assertEquals(oneByte, ByteBuffer.wrap(map.get(oneByte)));

    map.put(tenKey, eightyEightVal);
    assertMapValues(2, 100, 1);
    Assert.assertEquals(oneByte, ByteBuffer.wrap(map.get(oneByte)));
    Assert.assertEquals(ByteBuffer.wrap(eightyEightVal), ByteBuffer.wrap(map.get(tenKey)));

    map.put(twoByte, oneByte.array());
    assertMapValues(2, 101, 2);
    Assert.assertEquals(ByteBuffer.wrap(eightyEightVal), ByteBuffer.wrap(map.get(tenKey)));
    Assert.assertEquals(oneByte, ByteBuffer.wrap(map.get(twoByte)));
  }

  private void assertMapValues(final int size, final int numBytes, final int evictionCount)
  {
    Assert.assertEquals(size, map.size());
    Assert.assertEquals(numBytes, map.getNumBytes());
    Assert.assertEquals(evictionCount, map.getEvictionCount());
  }
}
