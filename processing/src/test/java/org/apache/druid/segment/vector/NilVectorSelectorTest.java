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

package org.apache.druid.segment.vector;

import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class NilVectorSelectorTest extends InitializedNullHandlingTest
{
  private static final int NUM_ROWS = 10_000;

  @Test
  public void testDefaultSizedVector()
  {
    testVectorSize(QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE);
  }

  @Test
  public void testCustomSizeVector()
  {
    testVectorSize(1024);
  }

  private static void testVectorSize(int vectorSize)
  {
    NoFilterVectorOffset offset = new NoFilterVectorOffset(vectorSize, 0, NUM_ROWS);
    testOffset(offset);
    WrappedRoaringBitmap bitmap = new WrappedRoaringBitmap();
    int numSet = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (ThreadLocalRandom.current().nextDouble() > 0.2) {
        bitmap.add(i);
        numSet++;
      }
    }
    BitmapVectorOffset bitmapOffset = new BitmapVectorOffset(vectorSize, bitmap.toImmutableBitmap(), 0, NUM_ROWS);
    testOffset(bitmapOffset);
  }

  private static void testOffset(VectorOffset offset)
  {
    NilVectorSelector nil = NilVectorSelector.create(offset);
    while (!offset.isDone()) {
      final int[] dict = nil.getRowVector();
      final long[] longs = nil.getLongVector();
      final double[] doubles = nil.getDoubleVector();
      final float[] floats = nil.getFloatVector();
      final boolean[] nulls = nil.getNullVector();
      final Object[] objects = nil.getObjectVector();

      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        Assert.assertEquals(0, dict[i]);
        Assert.assertEquals(0L, longs[i]);
        Assert.assertEquals(0.0, doubles[i], 0.0);
        Assert.assertEquals(0f, floats[i], 0.0);
        Assert.assertEquals(NullHandling.sqlCompatible(), nulls[i]);
        Assert.assertNull(objects[i]);
      }
      offset.advance();
    }
  }
}
