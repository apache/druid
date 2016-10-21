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

package io.druid.segment;

import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.junit.Test;

import static io.druid.segment.IntIteratorUtils.skip;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IntIteratorUtilsTest
{

  @Test
  public void testSkip()
  {
    assertEquals(0, skip(IntIterators.EMPTY_ITERATOR, 5));
    assertEquals(0, skip(IntIterators.EMPTY_ITERATOR, 0));

    IntListIterator it = IntIterators.fromTo(0, 10);
    assertEquals(3, skip(it, 3));
    assertEquals(3, it.nextInt());
    assertEquals(6, skip(it, 100));
    assertEquals(0, skip(it, 100));
    assertFalse(it.hasNext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeSkipArgument()
  {
    skip(IntIterators.fromTo(0, 10), -1);
  }
}
