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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;

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

    Iterator<ByteBuffer> it = map.keySet().iterator();
    while(it.hasNext()) {
      ByteBuffer buf = it.next();
      if(buf.remaining() == 10) {
        it.remove();
      }
    }
    assertMapValues(1, 3, 2);

    map.remove(twoByte);
    assertMapValues(0, 0, 2);
  }

  private void assertMapValues(final int size, final int numBytes, final int evictionCount)
  {
    Assert.assertEquals(size, map.size());
    Assert.assertEquals(numBytes, map.getNumBytes());
    Assert.assertEquals(evictionCount, map.getEvictionCount());
  }
}
