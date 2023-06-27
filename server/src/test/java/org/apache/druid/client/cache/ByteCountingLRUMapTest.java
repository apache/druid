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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 */
class ByteCountingLRUMapTest
{
  private ByteCountingLRUMap map;

  @BeforeEach
  public void setUp()
  {
    map = new ByteCountingLRUMap(100);
  }

  @Test
  void testSanity()
  {
    final ByteBuffer tenKey = ByteBuffer.allocate(10);
    final byte[] eightyEightVal = ByteBuffer.allocate(88).array();
    final byte[] eightNineNineVal = ByteBuffer.allocate(89).array();
    final ByteBuffer oneByte = ByteBuffer.allocate(1);
    final ByteBuffer twoByte = ByteBuffer.allocate(2);

    assertMapValues(0, 0, 0);
    map.put(tenKey, eightNineNineVal);
    assertMapValues(1, 99, 0);
    assertEquals(ByteBuffer.wrap(eightNineNineVal), ByteBuffer.wrap(map.get(tenKey)));

    map.put(oneByte, oneByte.array());
    assertMapValues(1, 2, 1);
    assertNull(map.get(tenKey));
    assertEquals(oneByte, ByteBuffer.wrap(map.get(oneByte)));

    map.put(tenKey, eightyEightVal);
    assertMapValues(2, 100, 1);
    assertEquals(oneByte, ByteBuffer.wrap(map.get(oneByte)));
    assertEquals(ByteBuffer.wrap(eightyEightVal), ByteBuffer.wrap(map.get(tenKey)));

    map.put(twoByte, oneByte.array());
    assertMapValues(1, 3, 3);
    assertNull(map.get(tenKey));
    assertEquals(oneByte, ByteBuffer.wrap(map.get(twoByte)));

    Iterator<ByteBuffer> it = map.keySet().iterator();
    List<ByteBuffer> toRemove = new ArrayList<>();
    while (it.hasNext()) {
      ByteBuffer buf = it.next();
      if (buf.remaining() == 10) {
        toRemove.add(buf);
      }
    }

    toRemove.forEach(buf -> map.remove(buf));
    assertMapValues(1, 3, 3);

    map.remove(twoByte);
    assertMapValues(0, 0, 3);
  }

  @Test
  void testSameKeyUpdate()
  {
    final ByteBuffer k = ByteBuffer.allocate(1);

    assertMapValues(0, 0, 0);
    map.put(k, new byte[1]); //-V6033: suppress "An item with the same key has already been added"
    map.put(k, new byte[2]); //-V6033
    map.put(k, new byte[5]); //-V6033
    map.put(k, new byte[3]); //-V6033
    assertMapValues(1, 4, 0);
  }

  private void assertMapValues(final int size, final int numBytes, final int evictionCount)
  {
    assertEquals(size, map.size());
    assertEquals(numBytes, map.getNumBytes());
    assertEquals(evictionCount, map.getEvictionCount());
  }
}
