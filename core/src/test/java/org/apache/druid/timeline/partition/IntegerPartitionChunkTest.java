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

package org.apache.druid.timeline.partition;

import org.junit.Assert;
import org.junit.Test;

public class IntegerPartitionChunkTest
{
  @Test
  public void testAbuts()
  {
    IntegerPartitionChunk<Integer> lhs = IntegerPartitionChunk.make(null, 10, 0, 1);

    Assert.assertTrue(lhs.abuts(IntegerPartitionChunk.make(10, null, 1, 2)));
    Assert.assertFalse(lhs.abuts(IntegerPartitionChunk.make(11, null, 2, 3)));
    Assert.assertFalse(lhs.abuts(IntegerPartitionChunk.make(null, null, 3, 4)));

    Assert.assertFalse(IntegerPartitionChunk.make(null, null, 0, 1)
                                            .abuts(IntegerPartitionChunk.make(null, null, 1, 2)));
  }

  @Test
  public void testIsStart()
  {
    Assert.assertTrue(IntegerPartitionChunk.make(null, 10, 0, 1).isStart());
    Assert.assertFalse(IntegerPartitionChunk.make(10, null, 0, 1).isStart());
    Assert.assertFalse(IntegerPartitionChunk.make(10, 11, 0, 1).isStart());
    Assert.assertTrue(IntegerPartitionChunk.make(null, null, 0, 1).isStart());
  }

  @Test
  public void testIsEnd()
  {
    Assert.assertFalse(IntegerPartitionChunk.make(null, 10, 0, 1).isEnd());
    Assert.assertTrue(IntegerPartitionChunk.make(10, null, 0, 1).isEnd());
    Assert.assertFalse(IntegerPartitionChunk.make(10, 11, 0, 1).isEnd());
    Assert.assertTrue(IntegerPartitionChunk.make(null, null, 0, 1).isEnd());
  }

  @Test
  public void testCompareTo()
  {
    //noinspection EqualsWithItself (the intention of this first test is specifically to call compareTo with itself)
    Assert.assertEquals(
        0,
        IntegerPartitionChunk.make(null, null, 0, 1).compareTo(IntegerPartitionChunk.make(null, null, 0, 1))
    );
    Assert.assertEquals(
        0,
        IntegerPartitionChunk.make(10, null, 0, 1).compareTo(IntegerPartitionChunk.make(10, null, 0, 2))
    );
    Assert.assertEquals(
        0,
        IntegerPartitionChunk.make(null, 10, 0, 1).compareTo(IntegerPartitionChunk.make(null, 10, 0, 2))
    );
    Assert.assertEquals(
        0,
        IntegerPartitionChunk.make(10, 11, 0, 1).compareTo(IntegerPartitionChunk.make(10, 11, 0, 2))
    );
    Assert.assertEquals(
        -1,
        IntegerPartitionChunk.make(null, 10, 0, 1).compareTo(IntegerPartitionChunk.make(10, null, 1, 2))
    );
    Assert.assertEquals(
        -1,
        IntegerPartitionChunk.make(11, 20, 0, 1).compareTo(IntegerPartitionChunk.make(20, 33, 1, 1))
    );
    Assert.assertEquals(
        1,
        IntegerPartitionChunk.make(20, 33, 1, 1).compareTo(IntegerPartitionChunk.make(11, 20, 0, 1))
    );
    Assert.assertEquals(
        1,
        IntegerPartitionChunk.make(10, null, 1, 1).compareTo(IntegerPartitionChunk.make(null, 10, 0, 1))
    );
  }

  @Test
  public void testEquals()
  {
    Assert.assertEquals(IntegerPartitionChunk.make(null, null, 0, 1), IntegerPartitionChunk.make(null, null, 0, 1));
    Assert.assertEquals(IntegerPartitionChunk.make(null, 10, 0, 1), IntegerPartitionChunk.make(null, 10, 0, 1));
    Assert.assertEquals(IntegerPartitionChunk.make(10, null, 0, 1), IntegerPartitionChunk.make(10, null, 0, 1));
    Assert.assertEquals(IntegerPartitionChunk.make(10, 11, 0, 1), IntegerPartitionChunk.make(10, 11, 0, 1));
  }
}
