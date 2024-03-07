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

package org.apache.druid.query.rowsandcols.semantic;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assume.assumeTrue;

public class ClusteredGroupPartitionerTest extends SemanticTestBase
{
  public ClusteredGroupPartitionerTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testEmpty()
  {
    RowsAndColumns rac = make(new MapOfColumnsRowsAndColumns(ImmutableMap.of(), 0));

    final ClusteredGroupPartitioner parter = ClusteredGroupPartitioner.fromRAC(rac);

    final List<String> cols = Collections.singletonList("notThere");
    Assert.assertArrayEquals(new int[]{}, parter.computeBoundaries(cols));
    Assert.assertTrue(parter.partitionOnBoundaries(cols).isEmpty());
  }

  @Test
  public void testDefaultClusteredGroupPartitioner()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    ));

    ClusteredGroupPartitioner parter = ClusteredGroupPartitioner.fromRAC(rac);

    int[] expectedBounds = new int[]{0, 3, 5, 6, 9};

    List<RowsAndColumnsHelper> expectations = Arrays.asList(
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{0, 0, 0})
            .expectColumn("unsorted", new int[]{3, 54, 21})
            .allColumnsRegistered(),
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{1, 1})
            .expectColumn("unsorted", new int[]{1, 5})
            .allColumnsRegistered(),
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{2})
            .expectColumn("unsorted", new int[]{54})
            .allColumnsRegistered(),
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{4, 4, 4})
            .expectColumn("unsorted", new int[]{2, 3, 92})
            .allColumnsRegistered()
    );

    final List<String> partCols = Collections.singletonList("sorted");
    Assert.assertArrayEquals(expectedBounds, parter.computeBoundaries(partCols));

    final Iterator<RowsAndColumns> partedChunks = parter.partitionOnBoundaries(partCols).iterator();
    for (RowsAndColumnsHelper expectation : expectations) {
      Assert.assertTrue(partedChunks.hasNext());
      expectation.validate(partedChunks.next());
    }
    Assert.assertFalse(partedChunks.hasNext());

    BiFunction<Integer, Integer, RowsAndColumnsHelper> singleHelperMaker =
        (sorted, unsorted) ->
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{sorted})
                .expectColumn("unsorted", new int[]{unsorted})
                .allColumnsRegistered();

    List<RowsAndColumnsHelper> unsortedExcpectations = Arrays.asList(
        singleHelperMaker.apply(0, 3),
        singleHelperMaker.apply(0, 54),
        singleHelperMaker.apply(0, 21),
        singleHelperMaker.apply(1, 1),
        singleHelperMaker.apply(1, 5),
        singleHelperMaker.apply(2, 54),
        singleHelperMaker.apply(4, 2),
        singleHelperMaker.apply(4, 3),
        singleHelperMaker.apply(4, 92)
    );

    final List<String> unsorted = Collections.singletonList("unsorted");
    final Iterator<RowsAndColumns> unsortedChunks = parter.partitionOnBoundaries(unsorted).iterator();
    for (RowsAndColumnsHelper expectation : unsortedExcpectations) {
      Assert.assertTrue(unsortedChunks.hasNext());
      expectation.validate(unsortedChunks.next());
    }
    Assert.assertFalse(unsortedChunks.hasNext());
  }

  @Test
  public void testDefaultClusteredGroupPartitionerWithNulls()
  {
    assumeTrue("testcase only enabled in sqlCompatible mode", NullHandling.sqlCompatible());

    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new ObjectArrayColumn(new Object[]{null, null, null, 1, 1, 2, 4, 4, 4}, ColumnType.LONG),
            "col_d", new ObjectArrayColumn(new Object[]{null, null, null, 1.0, 1.0, 2.0, 4.0, 4.0, 4.0}, ColumnType.DOUBLE),
            "col_f", new ObjectArrayColumn(new Object[]{null, null, null, 1.0f, 1.0f, 2.0f, 4.0f, 4.0f, 4.0f}, ColumnType.FLOAT),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    ));

    ClusteredGroupPartitioner parter = ClusteredGroupPartitioner.fromRAC(rac);

    int[] expectedBounds = new int[]{0, 3, 5, 6, 9};

    List<RowsAndColumnsHelper> expectations = Arrays.asList(
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new Object[]{null, null, null}, ColumnType.LONG)
            .expectColumn("col_d", new Object[]{null, null, null}, ColumnType.DOUBLE)
            .expectColumn("col_f", new Object[]{null, null, null}, ColumnType.FLOAT)
            .expectColumn("unsorted", new int[]{3, 54, 21})
            .allColumnsRegistered(),
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{1, 1})
            .expectColumn("col_d", new double[]{1.0, 1.0})
            .expectColumn("col_f", new float[]{1.0f, 1.0f})
            .expectColumn("unsorted", new int[]{1, 5})
            .allColumnsRegistered(),
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{2})
            .expectColumn("col_d", new double[]{2.0})
            .expectColumn("col_f", new float[]{2.0f})
            .expectColumn("unsorted", new int[]{54})
            .allColumnsRegistered(),
        new RowsAndColumnsHelper()
            .expectColumn("sorted", new int[]{4, 4, 4})
            .expectColumn("col_d", new double[]{4.0, 4.0, 4.0})
            .expectColumn("col_f", new float[]{4.0f, 4.0f, 4.0f})
            .expectColumn("unsorted", new int[]{2, 3, 92})
            .allColumnsRegistered()
    );

    final List<String> partCols = Collections.singletonList("sorted");
    Assert.assertArrayEquals(expectedBounds, parter.computeBoundaries(partCols));

    final Iterator<RowsAndColumns> partedChunks = parter.partitionOnBoundaries(partCols).iterator();
    for (RowsAndColumnsHelper expectation : expectations) {
      Assert.assertTrue(partedChunks.hasNext());
      expectation.validate(partedChunks.next());
    }
    Assert.assertFalse(partedChunks.hasNext());
  }
}
