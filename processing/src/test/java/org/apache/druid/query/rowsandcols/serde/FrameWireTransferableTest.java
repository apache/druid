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

package org.apache.druid.query.rowsandcols.serde;

import org.apache.druid.frame.wire.FrameWireTransferable;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumnsTest;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.RowBasedFrameRowsAndColumnsTest;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class FrameWireTransferableTest
{
  private static final MapOfColumnsRowsAndColumns INPUT_RAC = MapOfColumnsRowsAndColumns
      .builder()
      .add("colA", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
      .add("colB", new IntArrayColumn(new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0}))
      .add(
          "colC",
          new ObjectArrayColumn(
              new Object[]{"foo", "bar", "baz", "spot", "automotive", "business", "a", "b", "c", "d"},
              ColumnType.STRING
          )
      )
      .build();

  private final DefaultObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void testColumnBasedFrame() throws Exception
  {
    final ColumnBasedFrameRowsAndColumns frameRac = ColumnBasedFrameRowsAndColumnsTest.buildFrame(INPUT_RAC);
    final FrameWireTransferable wt = new FrameWireTransferable(frameRac.getFrame(), frameRac.getSignature());

    // Serialize
    final WireTransferable.ByteArrayOffsetAndLen serialized = wt.serializedBytes(objectMapper);
    final byte[] bytes = new byte[serialized.getLength()];
    System.arraycopy(serialized.getArray(), serialized.getOffset(), bytes, 0, serialized.getLength());

    // Deserialize
    final FrameWireTransferable.Deserializer deserializer = new FrameWireTransferable.Deserializer();
    final RowsAndColumns deserialized = deserializer.deserialize(objectMapper, ByteBufferUtils.wrapLE(bytes));

    Assert.assertTrue(deserialized instanceof ColumnBasedFrameRowsAndColumns);
    Assert.assertEquals(frameRac, deserialized);
  }

  @Test
  public void testColumnBasedFrame_withoutSignature() throws Exception
  {
    final ColumnBasedFrameRowsAndColumns frameRac = ColumnBasedFrameRowsAndColumnsTest.buildFrame(INPUT_RAC);
    final FrameWireTransferable wt = new FrameWireTransferable(frameRac.getFrame(), null);

    // Serialize
    final WireTransferable.ByteArrayOffsetAndLen serialized = wt.serializedBytes(objectMapper);
    final byte[] bytes = new byte[serialized.getLength()];
    System.arraycopy(serialized.getArray(), serialized.getOffset(), bytes, 0, serialized.getLength());

    // Deserialize
    final FrameWireTransferable.Deserializer deserializer = new FrameWireTransferable.Deserializer();
    final RowsAndColumns deserialized = deserializer.deserialize(objectMapper, ByteBufferUtils.wrapLE(bytes));

    Assert.assertTrue(deserialized instanceof ColumnBasedFrameRowsAndColumns);
    Assert.assertEquals(frameRac.getFrame(), ((ColumnBasedFrameRowsAndColumns) deserialized).getFrame());
    Assert.assertFalse(((ColumnBasedFrameRowsAndColumns) deserialized).hasSignature());
  }

  @Test
  public void testRowBasedFrame() throws Exception
  {
    final RowBasedFrameRowsAndColumns frameRac = RowBasedFrameRowsAndColumnsTest.MAKER.apply(INPUT_RAC);
    final FrameWireTransferable wt = new FrameWireTransferable(frameRac.getFrame(), frameRac.getSignature());

    // Serialize
    final WireTransferable.ByteArrayOffsetAndLen serialized = wt.serializedBytes(objectMapper);
    final byte[] bytes = new byte[serialized.getLength()];
    System.arraycopy(serialized.getArray(), serialized.getOffset(), bytes, 0, serialized.getLength());

    // Deserialize
    final FrameWireTransferable.Deserializer deserializer = new FrameWireTransferable.Deserializer();
    final RowsAndColumns deserialized = deserializer.deserialize(objectMapper, ByteBufferUtils.wrapLE(bytes));

    Assert.assertTrue(deserialized instanceof RowBasedFrameRowsAndColumns);
    Assert.assertEquals(frameRac, deserialized);
  }

  @Test
  public void testColumnBasedFrameViaAs() throws Exception
  {
    final ColumnBasedFrameRowsAndColumns frameRac = ColumnBasedFrameRowsAndColumnsTest.buildFrame(INPUT_RAC);

    // Get WireTransferable via as()
    final WireTransferable wt = frameRac.as(WireTransferable.class);
    Assert.assertNotNull(wt);

    // Serialize
    final WireTransferable.ByteArrayOffsetAndLen serialized = wt.serializedBytes(objectMapper);
    final byte[] bytes = new byte[serialized.getLength()];
    System.arraycopy(serialized.getArray(), serialized.getOffset(), bytes, 0, serialized.getLength());

    // Deserialize
    final FrameWireTransferable.Deserializer deserializer = new FrameWireTransferable.Deserializer();
    final RowsAndColumns deserialized = deserializer.deserialize(objectMapper, ByteBufferUtils.wrapLE(bytes));

    Assert.assertTrue(deserialized instanceof ColumnBasedFrameRowsAndColumns);
    Assert.assertEquals(frameRac, deserialized);
  }

  @Test
  public void testRowBasedFrameViaAs() throws Exception
  {
    final RowBasedFrameRowsAndColumns frameRac = RowBasedFrameRowsAndColumnsTest.MAKER.apply(INPUT_RAC);

    // Get WireTransferable via as()
    final WireTransferable wt = frameRac.as(WireTransferable.class);
    Assert.assertNotNull(wt);

    // Serialize
    final WireTransferable.ByteArrayOffsetAndLen serialized = wt.serializedBytes(objectMapper);
    final byte[] bytes = new byte[serialized.getLength()];
    System.arraycopy(serialized.getArray(), serialized.getOffset(), bytes, 0, serialized.getLength());

    // Deserialize
    final FrameWireTransferable.Deserializer deserializer = new FrameWireTransferable.Deserializer();
    final RowsAndColumns deserialized = deserializer.deserialize(objectMapper, ByteBufferUtils.wrapLE(bytes));

    Assert.assertTrue(deserialized instanceof RowBasedFrameRowsAndColumns);
    Assert.assertEquals(frameRac, deserialized);
  }
}
