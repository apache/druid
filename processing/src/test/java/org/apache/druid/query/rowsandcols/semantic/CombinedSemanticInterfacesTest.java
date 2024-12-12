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
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Place where tests can live that are testing the interactions of multiple semantic interfaces
 */
@SuppressWarnings("ConstantConditions")
public class CombinedSemanticInterfacesTest extends SemanticTestBase
{
  public CombinedSemanticInterfacesTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testColumnSelectorFactoryMakeColumnValueSelectorNonExistentColumn()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "some", new IntArrayColumn(new int[] {3, 54, 21, 1, 5, 54, 2, 3, 92}))));
    AtomicInteger currRow = new AtomicInteger();
    ColumnSelectorFactory csfm = ColumnSelectorFactoryMaker.fromRAC(rac).make(currRow);

    assertEquals(DimensionSelector.nilSelector(), csfm.makeColumnValueSelector("nonexistent"));
  }

  @Test
  public void testColumnSelectorFactoryGetColumnCapabilitiesNonExistentColumn()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "some", new IntArrayColumn(new int[] {3, 54, 21, 1, 5, 54, 2, 3, 92}))));
    AtomicInteger currRow = new AtomicInteger();
    ColumnSelectorFactory csfm = ColumnSelectorFactoryMaker.fromRAC(rac).make(currRow);

    assertNull(csfm.getColumnCapabilities("nonexistent"));
  }

  /**
   * Tests a relatively common series of operations for window functions: partition -> aggregate -> sort
   */
  @Test
  public void testPartitionAggregateAndSortTest()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    ));

    ClusteredGroupPartitioner parter = rac.as(ClusteredGroupPartitioner.class);
    if (parter == null) {
      parter = new DefaultClusteredGroupPartitioner(rac);
    }

    final ArrayList<RowsAndColumns> partitioned = parter.partitionOnBoundaries(Collections.singletonList("sorted"));
    Assert.assertEquals(4, partitioned.size());

    NaiveSortMaker.NaiveSorter sorter = null;
    for (RowsAndColumns rowsAndColumns : partitioned) {
      final FramedOnHeapAggregatable aggregatable = FramedOnHeapAggregatable.fromRAC(rowsAndColumns);
      final RowsAndColumns aggedRAC = aggregatable.aggregateAll(
          WindowFrame.unbounded(),
          new AggregatorFactory[]{new LongSumAggregatorFactory("sum", "unsorted")}
      );
      if (sorter == null) {
        sorter = NaiveSortMaker.fromRAC(aggedRAC).make(ColumnWithDirection.ascending("unsorted"));
      } else {
        Assert.assertNull(sorter.moreData(aggedRAC));
      }
    }
    Assert.assertNotNull(sorter);

    final RowsAndColumns completed = sorter.complete();
    Assert.assertNotNull(completed);

    new RowsAndColumnsHelper()
        .expectColumn("sorted", new int[]{1, 4, 0, 4, 1, 0, 0, 2, 4})
        .expectColumn("unsorted", new int[]{1, 2, 3, 3, 5, 21, 54, 54, 92})
        .expectColumn("sum", new long[]{6, 97, 78, 97, 6, 78, 78, 54, 97})
        .allColumnsRegistered()
        .validate(completed);
  }

  /**
   * Tests a relatively common series of operations for window functions: partition -> aggregate
   */
  @Test
  public void testPartitionThenAggregateNoSortTest()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    ));

    ClusteredGroupPartitioner parter = ClusteredGroupPartitioner.fromRAC(rac);

    final ArrayList<RowsAndColumns> partitioned = parter.partitionOnBoundaries(Collections.singletonList("sorted"));
    Assert.assertEquals(4, partitioned.size());

    Map<String, ObjectArrayColumn> outputCols = new LinkedHashMap<>();
    outputCols.put("sorted", new ObjectArrayColumn(new Object[9], ColumnType.LONG));
    outputCols.put("unsorted", new ObjectArrayColumn(new Object[9], ColumnType.LONG));
    outputCols.put("sum", new ObjectArrayColumn(new Object[9], ColumnType.LONG));

    int rowCounter = 0;
    for (RowsAndColumns rowsAndColumns : partitioned) {
      final FramedOnHeapAggregatable aggregatable = FramedOnHeapAggregatable.fromRAC(rowsAndColumns);
      final RowsAndColumns aggedRAC = aggregatable.aggregateAll(
          WindowFrame.unbounded(),
          new AggregatorFactory[]{new LongSumAggregatorFactory("sum", "unsorted")}
      );
      for (Map.Entry<String, ObjectArrayColumn> entry : outputCols.entrySet()) {
        Object[] objArray = entry.getValue().getObjects();
        final ColumnAccessor columnAccessor = aggedRAC.findColumn(entry.getKey()).toAccessor();
        for (int i = 0; i < columnAccessor.numRows(); ++i) {
          objArray[rowCounter + i] = columnAccessor.getObject(i);
        }
      }
      rowCounter += aggedRAC.numRows();
    }

    RowsAndColumns completed = MapOfColumnsRowsAndColumns.fromMap(outputCols);

    new RowsAndColumnsHelper()
        .expectColumn("sorted", new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4})
        .expectColumn("unsorted", new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        .expectColumn("sum", new long[]{78, 78, 78, 6, 6, 54, 97, 97, 97})
        .allColumnsRegistered()
        .validate(completed);
  }
}
