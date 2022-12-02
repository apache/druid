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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class SortedGroupPartitionerTest extends SemanticTestBase
{
  public SortedGroupPartitionerTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testDefaultSortedGroupPartitioner()
  {
    RowsAndColumns rac = make(MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    ));

    SortedGroupPartitioner parter = rac.as(SortedGroupPartitioner.class);
    if (parter == null) {
      parter = new DefaultSortedGroupPartitioner(rac);
    }

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
}