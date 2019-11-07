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
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class NumericNullColumnSelectorTest
{
  private final int numRows = 1024;
  private final int vectorSize = 128;
  private final SimpleAscendingOffset offset = new SimpleAscendingOffset(numRows);
  private final NoFilterVectorOffset vectorOffset = new NoFilterVectorOffset(vectorSize, 0, numRows);
  private ImmutableBitmap bitmap;

  @Before
  public void setup()
  {
    WrappedRoaringBitmap mutable = new WrappedRoaringBitmap();
    for (int i = 0; i < numRows; i++) {
      if (ThreadLocalRandom.current().nextDouble(0.0, 1.0) > 0.7) {
        mutable.add(i);
      }
    }
    bitmap = mutable.toImmutableBitmap();
  }

  @Test
  public void testLongSelectorWithNullsCanResetOffset()
  {
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

  @Test
  public void testFloatSelectorWithNullsCanResetOffset()
  {
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
  }

  @Test
  public void testDoubleSelectorWithNullsCanResetOffset()
  {
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

  public static void assertOffsetCanReset(
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
  }

  public static void assertVectorOffsetCanReset(
      VectorValueSelector selector,
      ImmutableBitmap bitmap,
      NoFilterVectorOffset readAllVectors
  )
  {
    boolean encounteredNull = false;
    boolean nullVector[];
    while (!readAllVectors.isDone()) {
      nullVector = selector.getNullVector();
      for (int i = readAllVectors.getStartOffset(); i < readAllVectors.getCurrentVectorSize(); i++) {
        Assert.assertEquals(bitmap.get(readAllVectors.getStartOffset() + i), nullVector[i]);
        encounteredNull |= nullVector[i];
      }
      readAllVectors.advance();
    }
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
  }
}
