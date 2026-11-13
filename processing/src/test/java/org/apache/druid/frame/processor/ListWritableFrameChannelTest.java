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

package org.apache.druid.frame.processor;

import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedCursorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListWritableFrameChannelTest extends InitializedNullHandlingTest
{
  private static final int MAX_FRAMES = 3;

  @Test
  public void test_write_upToMaxFrames_collectsFramesInOrder() throws Exception
  {
    final List<Frame> frames = makeFrames(MAX_FRAMES);
    final ListWritableFrameChannel channel = new ListWritableFrameChannel(MAX_FRAMES);

    for (final Frame frame : frames) {
      Assertions.assertTrue(channel.writabilityFuture().isDone());
      channel.write(frame);
    }
    channel.close();

    Assertions.assertFalse(channel.isOverflowed());
    Assertions.assertEquals(frames, channel.getFrames());
  }

  @Test
  public void test_write_beyondMaxFrames_overflows() throws Exception
  {
    final ListWritableFrameChannel channel = new ListWritableFrameChannel(MAX_FRAMES);
    for (final Frame frame : makeFrames(MAX_FRAMES + 2)) {
      Assertions.assertTrue(channel.writabilityFuture().isDone(), "stays writable past the cap");
      channel.write(frame);
    }
    channel.close();

    Assertions.assertTrue(channel.isOverflowed());
    Assertions.assertThrows(DruidException.class, channel::getFrames);
  }

  @Test
  public void test_getFrames_beforeClose_throws()
  {
    final ListWritableFrameChannel channel = new ListWritableFrameChannel(MAX_FRAMES);
    Assertions.assertThrows(DruidException.class, channel::getFrames);
    Assertions.assertThrows(DruidException.class, channel::isOverflowed);
  }

  @Test
  public void test_getFrames_afterFail_throws() throws Exception
  {
    final ListWritableFrameChannel channel = new ListWritableFrameChannel(MAX_FRAMES);
    channel.write(makeFrames(1).get(0));
    channel.fail(null);
    channel.close();

    Assertions.assertThrows(DruidException.class, channel::getFrames);
  }

  @Test
  public void test_write_afterClose_throws()
  {
    final ListWritableFrameChannel channel = new ListWritableFrameChannel(MAX_FRAMES);
    channel.close();

    final Frame frame = makeFrames(1).get(0);
    Assertions.assertThrows(DruidException.class, () -> channel.write(frame));
  }

  @Test
  public void test_write_nonFrame_throws()
  {
    final ListWritableFrameChannel channel = new ListWritableFrameChannel(MAX_FRAMES);
    final RowsAndColumns rac = Mockito.mock(RowsAndColumns.class);

    Assertions.assertThrows(DruidException.class, () -> channel.write(rac, WritableFrameChannel.NO_PARTITION));
  }

  /**
   * Returns {@code numFrames} single-row frames.
   */
  private static List<Frame> makeFrames(final int numFrames)
  {
    final RowSignature signature = RowSignature.builder().add("x", ColumnType.LONG).build();
    final List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numFrames; i++) {
      rows.add(new MapBasedRow(0L, Map.of("x", (long) i)));
    }

    final RowBasedCursorFactory<Row> cursorFactory =
        new RowBasedCursorFactory<>(Sequences.simple(rows), RowAdapters.standardRow(), signature);

    return FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                               .maxRowsPerFrame(1)
                               .frameType(FrameType.latestRowBased())
                               .frames()
                               .toList();
  }
}
