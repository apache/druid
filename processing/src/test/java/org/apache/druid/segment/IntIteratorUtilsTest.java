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

package org.apache.druid.segment;

import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import org.junit.Assert;
import org.junit.Test;

public class IntIteratorUtilsTest
{

  @Test
  public void testSkip()
  {
    Assert.assertEquals(0, IntIteratorUtils.skip(IntIterators.EMPTY_ITERATOR, 5));
    Assert.assertEquals(0, IntIteratorUtils.skip(IntIterators.EMPTY_ITERATOR, 0));

    IntListIterator it = IntIterators.fromTo(0, 10);
    Assert.assertEquals(3, IntIteratorUtils.skip(it, 3));
    Assert.assertEquals(3, it.nextInt());
    Assert.assertEquals(6, IntIteratorUtils.skip(it, 100));
    Assert.assertEquals(0, IntIteratorUtils.skip(it, 100));
    Assert.assertFalse(it.hasNext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeSkipArgument()
  {
    IntIteratorUtils.skip(IntIterators.fromTo(0, 10), -1);
  }
}
