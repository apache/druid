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

package io.druid.timeline.partition;

import org.junit.Assert;
import org.junit.Test;

import static io.druid.timeline.partition.IntegerPartitionChunk.make;

/**
 */
public class IntegerPartitionChunkTest
{
  @Test
  public void testAbuts() throws Exception
  {
    IntegerPartitionChunk<Integer> lhs = make(null, 10, 0, 1);

    Assert.assertTrue(lhs.abuts(make(10, null, 1, 2)));
    Assert.assertFalse(lhs.abuts(make(11, null, 2, 3)));
    Assert.assertFalse(lhs.abuts(make(null, null, 3, 4)));

    Assert.assertFalse(make(null, null, 0, 1).abuts(make(null, null, 1, 2)));
  }

  @Test
  public void testIsStart() throws Exception
  {
    Assert.assertTrue(make(null, 10, 0, 1).isStart());
    Assert.assertFalse(make(10, null, 0, 1).isStart());
    Assert.assertFalse(make(10, 11, 0, 1).isStart());
    Assert.assertTrue(make(null, null, 0, 1).isStart());
  }

  @Test
  public void testIsEnd() throws Exception
  {
    Assert.assertFalse(make(null, 10, 0, 1).isEnd());
    Assert.assertTrue(make(10, null, 0, 1).isEnd());
    Assert.assertFalse(make(10, 11, 0, 1).isEnd());
    Assert.assertTrue(make(null, null, 0, 1).isEnd());
  }

  @Test
  public void testCompareTo() throws Exception
  {
    Assert.assertEquals(0, make(null, null, 0, 1).compareTo(make(null, null, 0, 1)));
    Assert.assertEquals(0, make(10, null, 0, 1).compareTo(make(10, null, 0, 2)));
    Assert.assertEquals(0, make(null, 10, 0, 1).compareTo(make(null, 10, 0, 2)));
    Assert.assertEquals(0, make(10, 11, 0, 1).compareTo(make(10, 11, 0, 2)));
    Assert.assertEquals(-1, make(null, 10, 0, 1).compareTo(make(10, null, 1, 2)));
    Assert.assertEquals(-1, make(11, 20, 0, 1).compareTo(make(20, 33, 1, 1)));
    Assert.assertEquals(1, make(20, 33, 1, 1).compareTo(make(11, 20, 0, 1)));
    Assert.assertEquals(1, make(10, null, 1, 1).compareTo(make(null, 10, 0, 1)));
  }

  @Test
  public void testEquals() throws Exception
  {
    Assert.assertEquals(make(null, null, 0, 1), make(null, null, 0, 1));
    Assert.assertEquals(make(null, 10, 0, 1), make(null, 10, 0, 1));
    Assert.assertEquals(make(10, null, 0, 1), make(10, null, 0, 1));
    Assert.assertEquals(make(10, 11, 0, 1), make(10, 11, 0, 1));
  }
}
