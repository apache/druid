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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryableIndexCursorHolderTest
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
        QueryableIndexCursorHolder.timeSearch(column, 0, 0, values.length)
    );

    Assert.assertEquals(
        2,
        QueryableIndexCursorHolder.timeSearch(column, 0, 2, values.length)
    );

    Assert.assertEquals(
        0,
        QueryableIndexCursorHolder.timeSearch(column, 0, 0, values.length / 2)
    );

    Assert.assertEquals(
        1,
        QueryableIndexCursorHolder.timeSearch(column, 1, 0, values.length)
    );

    Assert.assertEquals(
        2,
        QueryableIndexCursorHolder.timeSearch(column, 1, 2, values.length)
    );

    Assert.assertEquals(
        1,
        QueryableIndexCursorHolder.timeSearch(column, 1, 0, values.length / 2)
    );

    Assert.assertEquals(
        1,
        QueryableIndexCursorHolder.timeSearch(column, 1, 1, 8)
    );

    Assert.assertEquals(
        8,
        QueryableIndexCursorHolder.timeSearch(column, 2, 0, values.length)
    );

    Assert.assertEquals(
        10,
        QueryableIndexCursorHolder.timeSearch(column, 10, 0, values.length)
    );

    Assert.assertEquals(
        11,
        QueryableIndexCursorHolder.timeSearch(column, 15, 0, values.length)
    );
  }

  @Test
  public void testProjectionTimeBoundaryInspector()
  {
    final DateTime startTime = Granularities.HOUR.bucketStart(DateTimes.nowUtc());
    final DimensionsSpec dims = DimensionsSpec.builder()
                                              .setDimensions(
                                                  Arrays.asList(
                                                      new StringDimensionSchema("a"),
                                                      new StringDimensionSchema("b"),
                                                      new LongDimensionSchema("c"),
                                                      new DoubleDimensionSchema("d")
                                                  )
                                              )
                                              .build();
    File tmp = FileUtils.createTempDir();
    final Closer closer = Closer.create();
    closer.register(tmp::delete);
    final List<InputRow> rows = Arrays.asList(
        new ListBasedInputRow(
            CursorFactoryProjectionTest.ROW_SIGNATURE,
            startTime,
            CursorFactoryProjectionTest.ROW_SIGNATURE.getColumnNames(),
            Arrays.asList("a", "aa", 1L, 1.0)
        ),
        new ListBasedInputRow(
            CursorFactoryProjectionTest.ROW_SIGNATURE,
            startTime.plusMinutes(2),
            CursorFactoryProjectionTest.ROW_SIGNATURE.getColumnNames(),
            Arrays.asList("a", "bb", 1L, 1.1, 1.1f)
        )
    );
    IndexBuilder bob = IndexBuilder.create()
                                   .tmpDir(tmp)
                                   .schema(
                                       IncrementalIndexSchema.builder()
                                                             .withDimensionsSpec(dims)
                                                             .withRollup(false)
                                                             .withMinTimestamp(startTime.getMillis())
                                                             .withProjections(
                                                                 Collections.singletonList(
                                                                     new AggregateProjectionSpec(
                                                                         "ab_hourly_cd_sum_time_ordered",
                                                                         null,
                                                                         VirtualColumns.create(
                                                                             Granularities.toVirtualColumn(
                                                                                 Granularities.HOUR,
                                                                                 "__gran"
                                                                             )
                                                                         ),
                                                                         Arrays.asList(
                                                                             new LongDimensionSchema("__gran"),
                                                                             new StringDimensionSchema("a"),
                                                                             new StringDimensionSchema("b")
                                                                         ),
                                                                         new AggregatorFactory[]{
                                                                             new LongSumAggregatorFactory(
                                                                                 "_c_sum",
                                                                                 "c"
                                                                             ),
                                                                             new DoubleSumAggregatorFactory("d", "d")
                                                                         }
                                                                     )
                                                                 )
                                                             )
                                                             .build()
                                   )
                                   .rows(rows);

    try (QueryableIndex index = bob.buildMMappedIndex()) {
      CursorBuildSpec buildSpec = CursorBuildSpec.builder()
                                                 .setGroupingColumns(ImmutableList.of("a", "b"))
                                                 .setPhysicalColumns(ImmutableSet.of("a", "b"))
                                                 .setAggregators(
                                                     ImmutableList.of(
                                                         new LongSumAggregatorFactory("c_sum", "c")
                                                     )
                                                 )
                                                 .setQueryContext(QueryContext.of(ImmutableMap.of(QueryContexts.FORCE_PROJECTION, true)))
                                                 .build();
      final CursorFactory cursorFactory = new QueryableIndexCursorFactory(index);

      try (final CursorHolder cursorHolder = cursorFactory.makeCursorHolder(buildSpec)) {
        final Cursor cursor = cursorHolder.asCursor();
        int rowCount = 0;
        while (!cursor.isDone()) {
          rowCount++;
          cursor.advance();
        }
        Assert.assertEquals(2, rowCount);
      }
    }
    finally {
      CloseableUtils.closeAndWrapExceptions(closer);
    }
  }
}
