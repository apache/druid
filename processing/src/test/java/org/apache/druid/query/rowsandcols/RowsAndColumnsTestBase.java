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

package org.apache.druid.query.rowsandcols;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.frame.AppendableMapOfColumns;
import org.apache.druid.query.rowsandcols.frame.MapOfColumnsRowsAndColumns;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * This base class is intended to serve as a common set of tests to validate specific RowsAndColumns implementations.
 * <p>
 * Different RowsAndColumns implementations will implement different of the semantic interfaces, this base class should
 * test all of the possible semantic interfaces that can be implemented.  By doing it this way, we can ensure that
 * new RowsAndColumns implementations meet all of the corners cases and other issues that have been previously found.
 * <p>
 * It is expected that this base class is going to grow quite large.  As it gets extra large, we could perhaps look
 * into whether one of the JUnit test runners could allow us to further sub-divide the test functionality into
 * semantic-interface-specific tests.  The ultimate goal, however, should be that a new RowsAndColumns implementation
 * can very simply take advantage of all of the tests by implementing the abstract
 * {@link #makeRowsAndColumns(MapOfColumnsRowsAndColumns)} method and be done.
 *
 * @param <T>
 */
public abstract class RowsAndColumnsTestBase<T extends RowsAndColumns>
{
  static {
    NullHandling.initializeForTests();
  }

  public abstract T makeRowsAndColumns(MapOfColumnsRowsAndColumns input);

  @Test
  public void testDefaultSortedGroupPartitioner()
  {
    T rac = makeRowsAndColumns(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    ));

    validateSortedGroupPartitioner("default", new DefaultSortedGroupPartitioner(rac));

    SortedGroupPartitioner specialized = rac.as(SortedGroupPartitioner.class);
    if (specialized != null) {
      validateSortedGroupPartitioner("specialized", specialized);
    }
  }

  private void validateSortedGroupPartitioner(String name, SortedGroupPartitioner parter)
  {

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
    Assert.assertArrayEquals(name, expectedBounds, parter.computeBoundaries(partCols));

    final Iterator<RowsAndColumns> partedChunks = parter.partitionOnBoundaries(partCols).iterator();
    for (RowsAndColumnsHelper expectation : expectations) {
      Assert.assertTrue(name, partedChunks.hasNext());
      expectation.validate(name, partedChunks.next());
    }
    Assert.assertFalse(name, partedChunks.hasNext());

    boolean exceptionThrown = false;
    try {
      parter.partitionOnBoundaries(Collections.singletonList("unsorted"));
    }
    catch (ISE ex) {
      Assert.assertEquals("Pre-sorted data required, rows[1] and [2] were not in order", ex.getMessage());
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testOnHeapAggregatable()
  {
    T rac = makeRowsAndColumns(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "incremented", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            "zeroesOut", new IntArrayColumn(new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        )
    ));

    validateOnHeapAggregatable("default", new DefaultOnHeapAggregatable(rac));

    OnHeapAggregatable specialized = rac.as(OnHeapAggregatable.class);
    if (specialized != null) {
      validateOnHeapAggregatable("specialized", specialized);
    }
  }

  private void validateOnHeapAggregatable(String name, OnHeapAggregatable agger)
  {
    final ArrayList<Object> results = agger.aggregateAll(Arrays.asList(
        new LongSumAggregatorFactory("incremented", "incremented"),
        new LongMaxAggregatorFactory("zeroesOutMax", "zeroesOut"),
        new LongMinAggregatorFactory("zeroesOutMin", "zeroesOut")
    ));

    Assert.assertEquals(name, 3, results.size());
    Assert.assertEquals(name, 55L, results.get(0));
    Assert.assertEquals(name, 82L, results.get(1));
    Assert.assertEquals(name, -90L, results.get(2));
  }

  @Test
  public void testAppendableRowsAndColumns()
  {
    T rac = makeRowsAndColumns(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "colA", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            "colB", new IntArrayColumn(new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        )
    ));

    validateAppendableRowsAndColumns("default", new AppendableMapOfColumns(rac));

    AppendableRowsAndColumns specialized = rac.as(AppendableRowsAndColumns.class);
    if (specialized != null) {
      validateAppendableRowsAndColumns("specialized", specialized);
    }
  }

  public void validateAppendableRowsAndColumns(String name, AppendableRowsAndColumns appender)
  {
    appender.addColumn("newCol", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));

    new RowsAndColumnsHelper()
        .expectColumn("colA", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        .expectColumn("colB", new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        .expectColumn("newCol", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
        .allColumnsRegistered()
        .validate(name, appender);
  }
}
