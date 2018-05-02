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

import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class RangePartitionChunkTest
{
  @Test
  public void testAbuts()
  {
    RangePartitionChunk<Integer> lhs = new RangePartitionChunk(Range.lessThan("10"), 0, 1);

    Assert.assertTrue(lhs.abuts(new RangePartitionChunk(Range.atLeast("10"), 1, 2)));
    Assert.assertFalse(lhs.abuts(new RangePartitionChunk(Range.atLeast("11"), 2, 3)));
    Assert.assertFalse(lhs.abuts(new RangePartitionChunk(Range.all(), 3, 4)));

    Assert.assertFalse(new RangePartitionChunk(Range.all(), 0, 1).abuts(new RangePartitionChunk(Range.all(), 1, 2)));
  }

  @Test
  public void testIsStart()
  {
    Assert.assertTrue(new RangePartitionChunk(Range.lessThan("10"), 0, 1).isStart());
    Assert.assertFalse(new RangePartitionChunk(Range.atLeast("10"), 0, 1).isStart());
    Assert.assertFalse(new RangePartitionChunk(Range.closedOpen("10", "11"), 0, 1).isStart());
    Assert.assertTrue(new RangePartitionChunk(Range.all(), 0, 1).isStart());
  }

  @Test
  public void testIsEnd()
  {
    Assert.assertFalse(new RangePartitionChunk(Range.lessThan("10"), 0, 1).isEnd());
    Assert.assertTrue(new RangePartitionChunk(Range.atLeast("10"), 0, 1).isEnd());
    Assert.assertFalse(new RangePartitionChunk(Range.closedOpen("10", "11"), 0, 1).isEnd());
    Assert.assertTrue(new RangePartitionChunk(Range.all(), 0, 1).isEnd());
  }

  @Test
  public void testCompareTo()
  {
    Assert.assertEquals(0, new RangePartitionChunk(Range.all(), 0, 1).compareTo(new RangePartitionChunk(Range.all(), 0, 2)));
    Assert.assertEquals(0, new RangePartitionChunk(Range.atLeast("10"), 0, 1).compareTo(new RangePartitionChunk(Range.atLeast("10"), 0, 2)));
    Assert.assertEquals(0, new RangePartitionChunk(Range.lessThan("10"), 1, 1).compareTo(new RangePartitionChunk(Range.lessThan("10"), 1, 2)));
    Assert.assertEquals(0, new RangePartitionChunk(Range.closedOpen("10", "11"), 1, 1).compareTo(new RangePartitionChunk(Range.closedOpen("10", "11"), 1, 2)));
    Assert.assertEquals(-1, new RangePartitionChunk(Range.atLeast("10"), 0, 1).compareTo(new RangePartitionChunk(Range.greaterThan("10"), 1, 2)));
    Assert.assertEquals(-1, new RangePartitionChunk(Range.closedOpen("11", "20"), 0, 1).compareTo(new RangePartitionChunk(Range.closedOpen("20", "33"), 1, 1)));
    Assert.assertEquals(1, new RangePartitionChunk(Range.closedOpen("20", "33"), 1, 1).compareTo(new RangePartitionChunk(Range.closedOpen("11", "20"), 0, 1)));
    Assert.assertEquals(1, new RangePartitionChunk(Range.atLeast("10"), 1, 1).compareTo(new RangePartitionChunk(Range.lessThan("10"), 0, 1)));
  }

  @Test
  public void testEquals()
  {
    Assert.assertEquals(new RangePartitionChunk(Range.all(), 0, 1), new RangePartitionChunk(Range.all(), 0, 1));
    Assert.assertEquals(new RangePartitionChunk(Range.lessThan("10"), 0, 1), new RangePartitionChunk(Range.lessThan("10"), 0, 1));
    Assert.assertEquals(new RangePartitionChunk(Range.atLeast("10"), 0, 1), new RangePartitionChunk(Range.atLeast("`0"), 0, 1));
    Assert.assertEquals(new RangePartitionChunk(Range.closedOpen("10", "11"), 0, 1), new RangePartitionChunk(Range.closedOpen("10", "11"), 0, 1));
  }
}
