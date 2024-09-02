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

package org.apache.druid.frame.field;

import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FieldReaderRACTest extends InitializedNullHandlingTest
{
  final DruidExceptionMatcher noArraysMatcher = DruidExceptionMatcher
      .defensive()
      .expectMessageIs("Can only work with single-valued strings, should use a COMPLEX or ARRAY typed Column instead");

  @Test
  public void testDataSet() throws IOException
  {
    final QueryableIndex index = TestIndex.getMMappedTestIndex();
    final CursorFactory cursorFactory = new QueryableIndexCursorFactory(index);
    final Frame frame = FrameTestUtil.cursorFactoryToFrame(cursorFactory, FrameType.ROW_BASED);

    final RowSignature siggy = cursorFactory.getRowSignature();
    final RowBasedFrameRowsAndColumns rowBasedRAC = new RowBasedFrameRowsAndColumns(frame, siggy);

    for (String columnName : siggy.getColumnNames()) {
      final ColumnHolder colHolder = index.getColumnHolder(columnName);
      final boolean multiValue = colHolder.getCapabilities().hasMultipleValues().isTrue();

      try (BaseColumn col = colHolder.getColumn()) {
        final ColumnAccessor racCol = rowBasedRAC.findColumn(columnName).toAccessor();

        final SimpleAscendingOffset offset = new SimpleAscendingOffset(racCol.numRows());
        final ColumnValueSelector<?> selector = col.makeColumnValueSelector(offset);
        while (offset.withinBounds()) {
          if (multiValue) {
            noArraysMatcher.assertThrowsAndMatches(() -> racCol.getObject(offset.getOffset()));
          } else {
            final Object racObj = racCol.getObject(offset.getOffset());
            Assert.assertEquals(racCol.isNull(offset.getOffset()), racCol.getObject(offset.getOffset()) == null);
            Assert.assertEquals(selector.getObject(), racObj);
          }
          offset.increment();
        }
      }
    }
  }
}
