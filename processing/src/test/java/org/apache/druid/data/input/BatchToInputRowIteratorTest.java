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

package org.apache.druid.data.input;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.rowsandcols.EmptyRowsAndColumns;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.DoubleArrayColumn;
import org.apache.druid.query.rowsandcols.column.LongArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchToInputRowIteratorTest
{
  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "millis", null),
      DimensionsSpec.builder()
                    .setDimensions(ImmutableList.of(
                        new StringDimensionSchema("name"),
                        new DoubleDimensionSchema("value")
                    ))
                    .build(),
      ColumnsFilter.all()
  );

  @Test
  public void testReadsMultipleBatchesWithReusableCursor() throws IOException
  {
    final AtomicBoolean closed = new AtomicBoolean();
    final CloseableIterator<RowsAndColumns> batches = batches(
        closed,
        new EmptyRowsAndColumns(),
        batch(new long[]{1_000L, 2_000L}, new Object[]{"alice", "bob"}, new double[]{1.5, 2.5}),
        batch(new long[]{3_000L}, new Object[]{"carol"}, new double[]{3.5})
    );

    try (BatchToInputRowIterator rows = new BatchToInputRowIterator(batches, INPUT_ROW_SCHEMA)) {
      final InputRow first = rows.next();
      Assertions.assertEquals(1_000L, first.getTimestampFromEpoch());
      Assertions.assertEquals(ImmutableList.of("name", "value"), first.getDimensions());
      Assertions.assertEquals(ImmutableList.of("alice"), first.getDimension("name"));
      Assertions.assertEquals(1.5, first.getMetric("value").doubleValue());

      final InputRow second = rows.next();
      Assertions.assertSame(first, second);
      Assertions.assertEquals(2_000L, second.getTimestampFromEpoch());
      Assertions.assertEquals("bob", second.getRaw("name"));

      final InputRow third = rows.next();
      Assertions.assertSame(first, third);
      Assertions.assertEquals(3_000L, third.getTimestampFromEpoch());
      Assertions.assertEquals("carol", third.getRaw("name"));
      Assertions.assertFalse(rows.hasNext());
    }

    Assertions.assertTrue(closed.get());
  }

  private static RowsAndColumns batch(
      final long[] timestamps,
      final Object[] names,
      final double[] values
  )
  {
    return MapOfColumnsRowsAndColumns.builder()
                                     .add("ts", new LongArrayColumn(timestamps))
                                     .add("name", new ObjectArrayColumn(names, ColumnType.STRING))
                                     .add("value", new DoubleArrayColumn(values))
                                     .build();
  }

  private static CloseableIterator<RowsAndColumns> batches(
      final AtomicBoolean closed,
      final RowsAndColumns... batches
  )
  {
    final Iterator<RowsAndColumns> delegate = ImmutableList.copyOf(batches).iterator();
    return new CloseableIterator<RowsAndColumns>()
    {
      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public RowsAndColumns next()
      {
        return delegate.next();
      }

      @Override
      public void close()
      {
        closed.set(true);
      }
    };
  }
}
