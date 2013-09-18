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
