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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.junit.Assert;
import org.junit.Test;

public class QueryableIndexCursorSequenceBuilderTest
{
  @Test
  public void testTimeSearch()
  {
    final int[] values = new int[]{0, 1, 1, 1, 1, 1, 1, 1, 5, 7, 10};
    final NumericColumn column = new NumericColumn()
    {
      @Override
      public int length()
      {
        return values.length;
      }

      @Override
      public long getLongSingleValueRow(int rowNum)
      {
        return values[rowNum];
      }

      @Override
      public void close()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
      {
        throw new UnsupportedOperationException();
      }
    };

    Assert.assertEquals(
        0,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 0, 0, values.length)
    );

    Assert.assertEquals(
        2,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 0, 2, values.length)
    );

    Assert.assertEquals(
        0,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 0, 0, values.length / 2)
    );

    Assert.assertEquals(
        1,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 1, 0, values.length)
    );

    Assert.assertEquals(
        2,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 1, 2, values.length)
    );

    Assert.assertEquals(
        1,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 1, 0, values.length / 2)
    );

    Assert.assertEquals(
        1,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 1, 1, 8)
    );

    Assert.assertEquals(
        8,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 2, 0, values.length)
    );

    Assert.assertEquals(
        10,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 10, 0, values.length)
    );

    Assert.assertEquals(
        11,
        QueryableIndexCursorSequenceBuilder.timeSearch(column, 15, 0, values.length)
    );
  }
}
