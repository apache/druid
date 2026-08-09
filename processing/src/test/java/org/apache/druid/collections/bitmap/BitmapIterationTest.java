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

package org.apache.druid.collections.bitmap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.IntIterator;

import java.util.List;

public class BitmapIterationTest
{
  public static List<BitmapFactory> factories()
  {
    return List.of(new BitSetBitmapFactory(), new ConciseBitmapFactory());
  }

  @ParameterizedTest
  @MethodSource("factories")
  public void testIteration(final BitmapFactory factory)
  {
    final MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    mutableBitmap.add(3);
    mutableBitmap.add(1);
    mutableBitmap.add(3);

    final IntIterator iterator = factory.makeImmutableBitmap(mutableBitmap).iterator();
    Assertions.assertTrue(iterator.hasNext());
    Assertions.assertEquals(1, iterator.next());
    Assertions.assertTrue(iterator.hasNext());
    Assertions.assertEquals(3, iterator.next());
    Assertions.assertFalse(iterator.hasNext());
  }

  @ParameterizedTest
  @MethodSource("factories")
  public void testEmptyIteration(final BitmapFactory factory)
  {
    final IntIterator iterator = factory.makeEmptyImmutableBitmap().iterator();
    Assertions.assertFalse(iterator.hasNext());
  }
}
