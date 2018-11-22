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

package org.apache.druid.query.select;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PagingOffsetTest
{
  @Test
  public void testZeroThreshold()
  {
    PagingOffset offset = PagingOffset.of(PagingOffset.toOffset(3, false), 0);
    Assert.assertEquals(3, offset.startOffset());
    Assert.assertEquals(3, offset.startDelta());
    Assert.assertArrayEquals(new int[]{}, toArray(offset));

    offset = PagingOffset.of(PagingOffset.toOffset(3, true), 0);
    Assert.assertEquals(-4, offset.startOffset());
    Assert.assertEquals(3, offset.startDelta());
    Assert.assertArrayEquals(new int[]{}, toArray(offset));
  }

  @Test
  public void testAscending()
  {
    PagingOffset offset = PagingOffset.of(PagingOffset.toOffset(3, false), 3);
    Assert.assertEquals(3, offset.startOffset());
    Assert.assertEquals(3, offset.startDelta());
    Assert.assertArrayEquals(new int[]{3, 4, 5}, toArray(offset));
  }

  @Test
  public void testDescending()
  {
    PagingOffset offset = PagingOffset.of(PagingOffset.toOffset(3, true), 3);
    Assert.assertEquals(-4, offset.startOffset());
    Assert.assertEquals(3, offset.startDelta());
    Assert.assertArrayEquals(new int[]{-4, -5, -6}, toArray(offset));
  }

  private int[] toArray(PagingOffset offset)
  {
    List<Integer> ints = new ArrayList<>();
    for (; offset.hasNext(); offset.next()) {
      ints.add(offset.current());
    }
    return Ints.toArray(ints);
  }
}
