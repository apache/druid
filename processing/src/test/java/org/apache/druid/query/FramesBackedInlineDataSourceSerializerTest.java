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

package org.apache.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class FramesBackedInlineDataSourceSerializerTest
{

  static {
    NullHandling.initializeForTests();
  }

  private static final Interval INTERVAL = Intervals.of("2000/P1Y");

  private static final RowSignature FOO_INLINE_SIGNATURE = RowSignature.builder()
                                                                       .addTimeColumn()
                                                                       .add("s", ColumnType.STRING)
                                                                       .add("n", ColumnType.LONG)
                                                                       .build();

  private static final IterableBackedInlineDataSource FOO_INLINE = IterableBackedInlineDataSource.fromIterable(
      ImmutableList.<Object[]>builder()
                   .add(new Object[]{INTERVAL.getStartMillis(), "x", 1})
                   .add(new Object[]{INTERVAL.getStartMillis(), "x", 2})
                   .add(new Object[]{INTERVAL.getStartMillis(), "y", 3})
                   .add(new Object[]{INTERVAL.getStartMillis(), "z", 4})
                   .build(),
      FOO_INLINE_SIGNATURE
  );

  private static final RowSignature BAR_INLINE_SIGNATURE = RowSignature.builder()
                                                                       .addTimeColumn()
                                                                       .add("s", ColumnType.STRING)
                                                                       .add("n", ColumnType.LONG)
                                                                       .build();


  private static final IterableBackedInlineDataSource BAR_INLINE = IterableBackedInlineDataSource.fromIterable(
      ImmutableList.<Object[]>builder()
                   .add(new Object[]{INTERVAL.getStartMillis(), "a", 1})
                   .add(new Object[]{INTERVAL.getStartMillis(), "a", 2})
                   .add(new Object[]{INTERVAL.getStartMillis(), "b", 3})
                   .add(new Object[]{INTERVAL.getStartMillis(), "c", 4})
                   .build(),
      BAR_INLINE_SIGNATURE
  );

  private static final RowSignature MULTI_VALUE_INLINE_SIGNATURE = RowSignature.builder()
                                                                               .addTimeColumn()
                                                                               .add("s", ColumnType.STRING_ARRAY)
                                                                               .add("n", ColumnType.LONG)
                                                                               .build();

  private static final IterableBackedInlineDataSource MULTI_VALUE_INLINE = IterableBackedInlineDataSource.fromIterable(
      ImmutableList.<Object[]>builder()
                   .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("a", "b"), 1})
                   .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("a", "c"), 2})
                   .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("b"), 3})
                   .add(new Object[]{INTERVAL.getStartMillis(), ImmutableList.of("c"), 4})
                   .build(),
      MULTI_VALUE_INLINE_SIGNATURE
  );

  ObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void serialize() throws JsonProcessingException
  {
    assertConversionBetweenFramesBackedAndIterableBackedInlineDataSource(
        convertToFramesBackedDataSource(FOO_INLINE, FOO_INLINE_SIGNATURE),
        FOO_INLINE
    );
    assertConversionBetweenFramesBackedAndIterableBackedInlineDataSource(
        convertToFramesBackedDataSource(BAR_INLINE, BAR_INLINE_SIGNATURE),
        BAR_INLINE
    );
    assertConversionBetweenFramesBackedAndIterableBackedInlineDataSource(
        convertToFramesBackedDataSource(MULTI_VALUE_INLINE, MULTI_VALUE_INLINE_SIGNATURE),
        MULTI_VALUE_INLINE
    );
  }

  private FramesBackedInlineDataSource convertToFramesBackedDataSource(
      IterableBackedInlineDataSource iterableBackedInlineDataSource,
      RowSignature rowSignature
  )
  {
    Cursor cursor = IterableRowsCursorHelper.getCursorFromIterable(
        iterableBackedInlineDataSource.getRows(),
        rowSignature
    );
    Frame frame = FrameCursorUtils.cursorToFrame(
        cursor,
        FrameWriters.makeFrameWriterFactory(
            FrameType.ROW_BASED,
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            rowSignature,
            new ArrayList<>(),
            true
        ),
        null
    );
    return new FramesBackedInlineDataSource(
        ImmutableList.of(new FrameSignaturePair(frame, rowSignature)),
        rowSignature
    );
  }

  private void assertConversionBetweenFramesBackedAndIterableBackedInlineDataSource(
      FramesBackedInlineDataSource framesBackedInlineDataSource,
      IterableBackedInlineDataSource iterableBackedInlineDataSource
  ) throws JsonProcessingException
  {
    String s = objectMapper.writeValueAsString(framesBackedInlineDataSource);
    DataSource back = objectMapper.readValue(s, DataSource.class);
    Assert.assertEquals(iterableBackedInlineDataSource, back);
  }
}
