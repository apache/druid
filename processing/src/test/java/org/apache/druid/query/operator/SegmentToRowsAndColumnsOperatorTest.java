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

package org.apache.druid.query.operator;

import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.segment.ArrayListSegment;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.TestSegmentForAs;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class SegmentToRowsAndColumnsOperatorTest
{
  @Test
  public void testSanity()
  {
    ArrayList<Object[]> rows = Lists.newArrayList(
        new Object[]{1, 2, "a"},
        new Object[]{1, 2, "b"}
    );

    ArrayListSegment<Object[]> segment = new ArrayListSegment<>(
        SegmentId.dummy("test"),
        rows,
        columnName -> objects -> objects[Integer.parseInt(columnName)],
        RowSignature.builder()
                    .add("0", ColumnType.LONG)
                    .add("1", ColumnType.DOUBLE)
                    .add("2", ColumnType.STRING).build()
    );
    final SegmentToRowsAndColumnsOperator op = new SegmentToRowsAndColumnsOperator(segment);

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("0", new long[]{1, 1})
                .expectColumn("1", new double[]{2, 2})
                .expectColumn("2", ColumnType.STRING, "a", "b")
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testNotShapeshiftable()
  {
    SegmentToRowsAndColumnsOperator op = new SegmentToRowsAndColumnsOperator(
        new TestSegmentForAs(SegmentId.dummy("test"), aClass -> {
          Assert.assertEquals(CloseableShapeshifter.class, aClass);
          //noinspection ReturnOfNull
          return null;
        })
    );

    boolean exceptionThrown = false;
    try {
      Operator.go(op, new ExceptionalReceiver());
    }
    catch (DruidException e) {
      Assert.assertEquals(
          e.getMessage(),
          "Segment [class org.apache.druid.segment.TestSegmentForAs] cannot shapeshift"
      );
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testCanShiftButNotToARAC()
  {
    AtomicBoolean closed = new AtomicBoolean(false);
    SegmentToRowsAndColumnsOperator op = new SegmentToRowsAndColumnsOperator(
        new TestSegmentForAs(SegmentId.dummy("test"), aClass -> {
          Assert.assertEquals(CloseableShapeshifter.class, aClass);
          return new CloseableShapeshifter()
          {
            @Nullable
            @Override
            public <T> T as(@Nonnull Class<T> clazz)
            {
              Assert.assertEquals(RowsAndColumns.class, clazz);
              return null;
            }

            @Override
            public void close()
            {
              closed.set(true);
            }
          };
        })
    );

    boolean exceptionThrown = false;
    try {
      Operator.go(op, new ExceptionalReceiver());
    }
    catch (ISE e) {
      Assert.assertEquals(
          e.getMessage(),
          "Cannot work with segment of type[class org.apache.druid.segment.TestSegmentForAs]"
      );
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    Assert.assertTrue(closed.get());
  }

  @Test
  public void testExceptionWhileClosing()
  {
    final MapOfColumnsRowsAndColumns expectedRac =
        MapOfColumnsRowsAndColumns.of("0", new IntArrayColumn(new int[]{0, 1}));
    AtomicBoolean closed = new AtomicBoolean(false);

    SegmentToRowsAndColumnsOperator op = new SegmentToRowsAndColumnsOperator(
        new TestSegmentForAs(SegmentId.dummy("test"), aClass -> {
          Assert.assertEquals(CloseableShapeshifter.class, aClass);
          return new CloseableShapeshifter()
          {
            @SuppressWarnings("unchecked")
            @Override
            public <T> T as(@Nonnull Class<T> clazz)
            {
              Assert.assertEquals(RowsAndColumns.class, clazz);
              return (T) expectedRac;
            }

            @Override
            public void close() throws IOException
            {
              closed.set(true);
              throw new IOException("ain't no thang");
            }
          };
        })
    );

    boolean exceptionThrown = false;
    try {
      new OperatorTestHelper()
          .withPushFn(() -> rac -> {
            Assert.assertSame(expectedRac, rac);
            return Operator.Signal.GO;
          })
          .runToCompletion(op);
    }
    catch (RE e) {
      Assert.assertEquals(
          e.getMessage(),
          "Problem closing resources for segment[test_-146136543-09-08T08:23:32.096Z_146140482-04-24T15:36:27.903Z_dummy_version]"
      );
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    Assert.assertTrue(closed.get());
  }
}
