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

package org.apache.druid.segment.data;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedRoaringBitmap;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.column.DoublesColumn;
import org.apache.druid.segment.column.FloatsColumn;
import org.apache.druid.segment.column.LongsColumn;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class NumericNullColumnSelectorTest
{
  private final int seed = 1337;
  private final Random rando = new Random(seed);
  private final int numBitmaps = 32;
  private final int numRows = 1024;
  private final int vectorSize = 128;
  private final SimpleAscendingOffset offset = new SimpleAscendingOffset(numRows);
  private final NoFilterVectorOffset vectorOffset = new NoFilterVectorOffset(vectorSize, 0, numRows);
  private final NoFilterOffsetThatCanBeMangledToTestOverlapping anotherVectorOffset =
      new NoFilterOffsetThatCanBeMangledToTestOverlapping(vectorSize, 0, numRows);
  private ImmutableBitmap[] bitmaps;

  @Before
  public void setup()
  {
    bitmaps = new ImmutableBitmap[numBitmaps];
    for (int bitmap = 0; bitmap < numBitmaps; bitmap++) {
      WrappedRoaringBitmap mutable = new WrappedRoaringBitmap();
      for (int i = 0; i < numRows; i++) {
        if (rando.nextDouble() > 0.2) {
          mutable.add(i);
        }
      }
      bitmaps[bitmap] = mutable.toImmutableBitmap();
    }
  }

  @Test
  public void testLongSelectorWithNullsCanResetOffset()
  {
    for (ImmutableBitmap bitmap : bitmaps) {
      ColumnarLongs longs = new ColumnarLongs()
      {
        @Override
        public int size()
        {
          return numRows;
        }

        @Override
        public long get(int index)
        {
          return ThreadLocalRandom.current().nextLong();
        }

        @Override
        public void close()
        {

        }
      };
      LongsColumn columnWithNulls = LongsColumn.create(longs, bitmap);
      ColumnValueSelector<?> selector = columnWithNulls.makeColumnValueSelector(offset);
      assertOffsetCanReset(selector, bitmap, offset);
      VectorValueSelector vectorSelector = columnWithNulls.makeVectorValueSelector(vectorOffset);
      assertVectorOffsetCanReset(vectorSelector, bitmap, vectorOffset);
    }
  }

  @Test
  public void testFloatSelectorWithNullsCanResetOffset()
  {
    for (ImmutableBitmap bitmap : bitmaps) {
      ColumnarFloats floats = new ColumnarFloats()
      {
        @Override
        public int size()
        {
          return numRows;
        }

        @Override
        public float get(int index)
        {
          return ThreadLocalRandom.current().nextFloat();
        }

        @Override
        public void close()
        {

        }
      };
      FloatsColumn columnWithNulls = FloatsColumn.create(floats, bitmap);
      ColumnValueSelector<?> selector = columnWithNulls.makeColumnValueSelector(offset);
      assertOffsetCanReset(selector, bitmap, offset);
      VectorValueSelector vectorSelector = columnWithNulls.makeVectorValueSelector(vectorOffset);
      assertVectorOffsetCanReset(vectorSelector, bitmap, vectorOffset);
      VectorValueSelector anotherSelector = columnWithNulls.makeVectorValueSelector(anotherVectorOffset);
      assertVectorChillWhenOffsetsOverlap(anotherSelector, bitmap, anotherVectorOffset);
    }
  }

  @Test
  public void testDoubleSelectorWithNullsCanResetOffset()
  {
    for (ImmutableBitmap bitmap : bitmaps) {
      ColumnarDoubles doubles = new ColumnarDoubles()
      {
        @Override
        public int size()
        {
          return numRows;
        }

        @Override
        public double get(int index)
        {
          return ThreadLocalRandom.current().nextDouble();
        }

        @Override
        public void close()
        {

        }
      };
      DoublesColumn columnWithNulls = DoublesColumn.create(doubles, bitmap);
      ColumnValueSelector<?> selector = columnWithNulls.makeColumnValueSelector(offset);
      assertOffsetCanReset(selector, bitmap, offset);
      VectorValueSelector vectorSelector = columnWithNulls.makeVectorValueSelector(vectorOffset);
      assertVectorOffsetCanReset(vectorSelector, bitmap, vectorOffset);
    }
  }

  private static void assertOffsetCanReset(
      BaseNullableColumnValueSelector selector,
      ImmutableBitmap bitmap,
      SimpleAscendingOffset readItAll
  )
  {
    boolean encounteredNull = false;
    while (readItAll.withinBounds()) {
      Assert.assertEquals(bitmap.get(readItAll.getOffset()), selector.isNull());
      encounteredNull |= selector.isNull();
      readItAll.increment();
    }
    readItAll.reset();
    Assert.assertTrue(encounteredNull);
    encounteredNull = false;
    while (readItAll.withinBounds()) {
      Assert.assertEquals(bitmap.get(readItAll.getOffset()), selector.isNull());
      encounteredNull |= selector.isNull();
      readItAll.increment();
    }
    Assert.assertTrue(encounteredNull);
    readItAll.reset();
  }

  private static void assertVectorOffsetCanReset(
      VectorValueSelector selector,
      ImmutableBitmap bitmap,
      NoFilterVectorOffset readAllVectors
  )
  {
    boolean encounteredNull = false;
    boolean[] nullVector;

    // read it all, advancing offset
    while (!readAllVectors.isDone()) {
      nullVector = selector.getNullVector();
      for (int i = readAllVectors.getStartOffset(); i < readAllVectors.getCurrentVectorSize(); i++) {
        Assert.assertEquals(bitmap.get(readAllVectors.getStartOffset() + i), nullVector[i]);
        encounteredNull |= nullVector[i];
      }
      readAllVectors.advance();
    }
    // reset and read it all again to make sure matches
    readAllVectors.reset();
    Assert.assertTrue(encounteredNull);
    encounteredNull = false;
    while (!readAllVectors.isDone()) {
      nullVector = selector.getNullVector();
      for (int i = readAllVectors.getStartOffset(); i < readAllVectors.getCurrentVectorSize(); i++) {
        Assert.assertEquals(bitmap.get(readAllVectors.getStartOffset() + i), nullVector[i]);
        encounteredNull |= nullVector[i];
      }
      readAllVectors.advance();
    }
    Assert.assertTrue(encounteredNull);
    readAllVectors.reset();
  }

  public static void assertVectorChillWhenOffsetsOverlap(
      VectorValueSelector selector,
      ImmutableBitmap bitmap,
      NoFilterOffsetThatCanBeMangledToTestOverlapping readAllVectors
  )
  {
    boolean encounteredNull = false;
    boolean[] nullVector;

    // test overlapping reads (should reset iterator anyway)
    readAllVectors.mangleOffset(0);
    nullVector = selector.getNullVector();
    for (int i = readAllVectors.getStartOffset(); i < readAllVectors.getCurrentVectorSize(); i++) {
      Assert.assertEquals(bitmap.get(readAllVectors.getStartOffset() + i), nullVector[i]);
      encounteredNull |= nullVector[i];
    }
    Assert.assertTrue(encounteredNull);
    // this can't currently happen, but we want to protect selectors in case offsets ever try to overlap
    readAllVectors.mangleOffset(1);

    nullVector = selector.getNullVector();
    for (int i = readAllVectors.getStartOffset(); i < readAllVectors.getCurrentVectorSize(); i++) {
      Assert.assertEquals(bitmap.get(readAllVectors.getStartOffset() + i), nullVector[i]);
      encounteredNull |= nullVector[i];
    }
    readAllVectors.reset();

    Assert.assertTrue(encounteredNull);
  }

  private static class NoFilterOffsetThatCanBeMangledToTestOverlapping implements VectorOffset
  {
    private final int maxVectorSize;
    private final int start;
    private final int end;
    private int theOffset;

    NoFilterOffsetThatCanBeMangledToTestOverlapping(final int maxVectorSize, final int start, final int end)
    {
      this.maxVectorSize = maxVectorSize;
      this.start = start;
      this.end = end;
      reset();
    }

    public void mangleOffset(int replacement)
    {
      theOffset = replacement;
    }

    @Override
    public int getId()
    {
      return theOffset;
    }

    @Override
    public void advance()
    {
      theOffset += maxVectorSize;
    }

    @Override
    public boolean isDone()
    {
      return theOffset >= end;
    }

    @Override
    public boolean isContiguous()
    {
      return true;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxVectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return Math.min(maxVectorSize, end - theOffset);
    }

    @Override
    public int getStartOffset()
    {
      return theOffset;
    }

    @Override
    public int[] getOffsets()
    {
      throw new UnsupportedOperationException("no filter");
    }

    @Override
    public void reset()
    {
      theOffset = start;
    }
  }
}
