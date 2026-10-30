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

import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class FilteredSegmentTest
{
  /** Window queries read segments as {@link RowsAndColumns}; a filtered segment provides them with its filter applied. */
  @Test
  public void testShapeshiftsToFilteredRowsAndColumns() throws Exception
  {
    final RowSignature signature = RowSignature.builder().add("dim", ColumnType.STRING).build();
    final InlineDataSource inline = InlineDataSource.fromIterable(
        List.of(new Object[]{"a"}, new Object[]{"b"}, new Object[]{"a"}),
        signature
    );
    final Segment segment = new FilteredSegment(
        new ArrayListSegment<>(new ArrayList<>(inline.getRowsAsList()), inline.rowAdapter(), signature),
        new SelectorDimFilter("dim", "b", null)
    );

    try (final CloseableShapeshifter shapeshifter = segment.as(CloseableShapeshifter.class)) {
      Assertions.assertNotNull(shapeshifter);
      final RowsAndColumns rac = Assertions.assertInstanceOf(RowsAndColumns.class, shapeshifter);
      Assertions.assertEquals(1, rac.numRows());
      Assertions.assertEquals("b", rac.findColumn("dim").toAccessor().getObject(0));
    }
  }
}
