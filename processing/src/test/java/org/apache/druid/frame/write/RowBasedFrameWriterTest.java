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

package org.apache.druid.frame.write;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.AppendableMemory;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.field.LongFieldWriter;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class RowBasedFrameWriterTest extends InitializedNullHandlingTest
{
  @Test
  public void test_addSelection_singleLargeRow()
  {
    final RowSignature signature =
        RowSignature.builder()
                    .add("n", ColumnType.LONG)
                    .add("s", ColumnType.STRING)
                    .build();

    final byte[] largeUtf8 = new byte[990000];
    Arrays.fill(largeUtf8, (byte) 'F');
    final String largeString = StringUtils.fromUtf8(largeUtf8);
    final Row largeRow = new MapBasedRow(0L, ImmutableMap.of("n", 3L, "s", largeString));

    final FrameWriterFactory frameWriterFactory = FrameWriters.makeRowBasedFrameWriterFactory(
        new ArenaMemoryAllocatorFactory(1_000_000),
        signature,
        ImmutableList.of(),
        false
    );

    final ColumnSelectorFactory columnSelectorFactory = RowBasedColumnSelectorFactory.create(
        RowAdapters.standardRow(),
        () -> largeRow,
        signature,
        false,
        false
    );

    final Frame frame;
    try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(columnSelectorFactory)) {
      Assert.assertTrue(frameWriter.addSelection());
      frame = Frame.wrap(frameWriter.toByteArray());
    }

    FrameTestUtil.assertRowsEqual(
        Sequences.simple(Collections.singletonList(ImmutableList.of(3L, largeString))),
        FrameTestUtil.readRowsFromCursorFactory(FrameReader.create(signature).makeCursorFactory(frame))
    );
  }

  @Test
  public void test_addSelection_withException()
  {
    String colName = "colName";
    String errorMsg = "Frame writer exception";

    final RowSignature signature = RowSignature.builder().add(colName, ColumnType.LONG).build();

    LongFieldWriter fieldWriter = EasyMock.mock(LongFieldWriter.class);
    EasyMock.expect(fieldWriter.writeTo(
        EasyMock.anyObject(),
        EasyMock.anyLong(),
        EasyMock.anyLong()
    )).andThrow(new RuntimeException(errorMsg));

    EasyMock.replay(fieldWriter);

    RowBasedFrameWriter rowBasedFrameWriter = new RowBasedFrameWriter(
        signature,
        Collections.emptyList(),
        ImmutableList.of(fieldWriter),
        null,
        null,
        AppendableMemory.create(HeapMemoryAllocator.unlimited()),
        AppendableMemory.create(HeapMemoryAllocator.unlimited())
    );

    InvalidFieldException expectedException = new InvalidFieldException.Builder()
        .column(colName)
        .errorMsg(errorMsg)
        .build();

    InvalidFieldException actualException = Assert.assertThrows(
        InvalidFieldException.class,
        rowBasedFrameWriter::addSelection
    );
    Assert.assertEquals(expectedException, actualException);
  }
}
