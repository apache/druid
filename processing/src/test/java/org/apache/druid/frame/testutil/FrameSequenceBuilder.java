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

package org.apache.druid.frame.testutil;

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility for making {@link Frame} instances for testing.
 */
public class FrameSequenceBuilder
{
  private final CursorFactory cursorFactory;

  private FrameType frameType = null;
  private MemoryAllocator allocator = HeapMemoryAllocator.unlimited();
  private List<KeyColumn> keyColumns = new ArrayList<>();
  private int maxRowsPerFrame = Integer.MAX_VALUE;
  private boolean populateRowNumber = false;

  private FrameSequenceBuilder(CursorFactory cursorFactory)
  {
    this.cursorFactory = cursorFactory;
  }

  public static FrameSequenceBuilder fromCursorFactory(final CursorFactory cursorFactory)
  {
    return new FrameSequenceBuilder(cursorFactory);
  }

  /**
   * Returns what {@link #signature()} would return if {@link #populateRowNumber()} is set.
   */
  public static RowSignature signatureWithRowNumber(final RowSignature signature)
  {
    return RowSignature.builder()
                       .addAll(signature)
                       .add(FrameTestUtil.ROW_NUMBER_COLUMN, ColumnType.LONG)
                       .build();
  }

  public FrameSequenceBuilder frameType(final FrameType frameType)
  {
    this.frameType = frameType;
    return this;
  }

  public FrameSequenceBuilder allocator(final MemoryAllocator allocator)
  {
    this.allocator = allocator;
    return this;
  }

  /**
   * Sorts each frame by the given columns. Does not do any sorting between frames.
   */
  public FrameSequenceBuilder sortBy(final List<KeyColumn> sortBy)
  {
    this.keyColumns = sortBy;
    return this;
  }

  /**
   * Limits each frame to the given size.
   */
  public FrameSequenceBuilder maxRowsPerFrame(final int maxRowsPerFrame)
  {
    this.maxRowsPerFrame = maxRowsPerFrame;
    return this;
  }

  public FrameSequenceBuilder populateRowNumber()
  {
    this.populateRowNumber = true;
    return this;
  }

  public RowSignature signature()
  {
    final RowSignature baseSignature;

    if (populateRowNumber) {
      baseSignature = signatureWithRowNumber(cursorFactory.getRowSignature());
    } else {
      baseSignature = cursorFactory.getRowSignature();
    }

    return FrameWriters.sortableSignature(baseSignature, keyColumns);
  }

  public Sequence<Frame> frames()
  {
    final FrameWriterFactory frameWriterFactory;
    if (FrameType.ROW_BASED.equals(frameType)) {
      frameWriterFactory = FrameWriters.makeRowBasedFrameWriterFactory(
          new SingleMemoryAllocatorFactory(allocator),
          signature(),
          keyColumns,
          false
      );
    } else if (FrameType.COLUMNAR.equals(frameType)) {
      frameWriterFactory = FrameWriters.makeColumnBasedFrameWriterFactory(
          new SingleMemoryAllocatorFactory(allocator),
          signature(),
          keyColumns
      );
    } else {
      throw DruidException.defensive("Unrecognized frame type");
    }

    final CursorHolder cursorHolder = FrameTestUtil.makeCursorForCursorFactory(cursorFactory, populateRowNumber);
    final Cursor cursor = cursorHolder.asCursor();
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Frame, Iterator<Frame>>()
        {
          @Override
          public Iterator<Frame> make()
          {
            final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

            return new Iterator<>()
            {
              @Override
              public boolean hasNext()
              {
                return !cursor.isDone();
              }

              @Override
              public Frame next()
              {
                if (cursor.isDone()) {
                  throw new NoSuchElementException();
                }

                try (final FrameWriter writer = frameWriterFactory.newFrameWriter(columnSelectorFactory)) {
                  while (!cursor.isDone()) {
                    if (!writer.addSelection()) {
                      if (writer.getNumRows() == 0) {
                        throw new FrameRowTooLargeException(allocator.capacity());
                      }

                      return makeFrame(writer);
                    }

                    cursor.advance();

                    if (writer.getNumRows() >= maxRowsPerFrame) {
                      return makeFrame(writer);
                    }
                  }

                  return makeFrame(writer);
                }
              }

              private Frame makeFrame(final FrameWriter writer)
              {
                return Frame.wrap(writer.toByteArray());
              }
            };
          }

          @Override
          public void cleanup(Iterator<Frame> iterFromMake)
          {
            // Nothing to do.
          }
        }
    ).withBaggage(cursorHolder);
  }
}
