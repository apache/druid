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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

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
    assertMapValues(1, 3, 3);
    Assert.assertEquals(null, map.get(tenKey));
    Assert.assertEquals(oneByte, ByteBuffer.wrap(map.get(twoByte)));

    Iterator<ByteBuffer> it = map.keySet().iterator();
    List<ByteBuffer> toRemove = Lists.newLinkedList();
    while(it.hasNext()) {
      ByteBuffer buf = it.next();
      if(buf.remaining() == 10) {
        toRemove.add(buf);
      }
    }
    for(ByteBuffer buf : toRemove) {
      map.remove(buf);
    }
    assertMapValues(1, 3, 3);

    map.remove(twoByte);
    assertMapValues(0, 0, 3);
  }

  @Test
  public void testSameKeyUpdate() throws Exception
  {
    final ByteBuffer k = ByteBuffer.allocate(1);

    assertMapValues(0, 0, 0);
    map.put(k, new byte[1]);
    map.put(k, new byte[2]);
    map.put(k, new byte[5]);
    map.put(k, new byte[3]);
    assertMapValues(1, 4, 0);
  }

  private void assertMapValues(final int size, final int numBytes, final int evictionCount)
  {
    Assert.assertEquals(size, map.size());
    Assert.assertEquals(numBytes, map.getNumBytes());
    Assert.assertEquals(evictionCount, map.getEvictionCount());
  }
}
