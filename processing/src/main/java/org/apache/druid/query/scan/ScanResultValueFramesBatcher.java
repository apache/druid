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

package org.apache.druid.query.scan;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.RowSignature;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ScanResultValueFramesBatcher
{

  // NOT THREAD SAFE
  public static Sequence<FrameSignaturePair> getBatchedSequence(
      final Sequence<ScanResultValue> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes,
      final RowSignature defaultRowSignature,
      final Function<RowSignature, Function<?, Object[]>> resultFormatMapper
  )
  {
    Closer closer = Closer.create();

    Iterable<FrameSignaturePair> retVal = () -> new Iterator<FrameSignaturePair>()
    {
      ScanResultValueIterator resultSequenceIterator = new ScanResultValueIterator(resultSequence);
      Cursor currentCursor = null;
      RowSignature currentRowSignature = null;
      RowSignature previousRowSignature = null;

      @Override
      public boolean hasNext()
      {
        return !done();
      }

      private boolean done()
      {
        return
            (currentCursor == null || currentCursor.isDone()) // Check if there are remaining events in the cursor
            && !(resultSequenceIterator.hasNext()); // Check if the ScanResultValue has no more events
      }


      @Override
      public FrameSignaturePair next()
      {
        // It would ensure that the cursor and the currentRowSignature is populated properly before we
        // start all the shenanigans
        populateCursor();
        boolean firstRowWritten = false;
        RowSignature modifiedRowSignature = useNestedForUnknownTypes
                                            ? FrameWriterUtils.replaceUnknownTypesWithNestedColumns(currentRowSignature)
                                            : currentRowSignature;
        FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
            FrameType.COLUMNAR,
            memoryAllocatorFactory,
            modifiedRowSignature,
            Collections.emptyList()
        );
        Frame frame;
        boolean compatible;
        final RowSignature writtenSignature = currentRowSignature;
        try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(new SettableCursorColumnSelectorFactory(
            () -> currentCursor,
            currentRowSignature
        ))) {
          //noinspection AssignmentUsedAsCondition
          while (compatible = populateCursor()) {
            if (!frameWriter.addSelection()) {
              break;
            }
            firstRowWritten = true;
            currentCursor.advance();
          }

          if (!firstRowWritten) {
            throw DruidException
                .forPersona(DruidException.Persona.DEVELOPER)
                .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                .build("Subquery's row size exceeds the frame size and therefore cannot write the subquery's "
                       + "row to the frame. This is a non-configurable static limit that can only be modified by the "
                       + "developer.");
          }
          frame = Frame.wrap(frameWriter.toByteArray());
        }

        return new FrameSignaturePair(frame, writtenSignature);
      }

      // Returns true if cursor was compatible with the previous cursor. This signifies the callers can batch calls
      // Idempotent if there is still some value in the current cursor
      // b) Cursor is not done()
      // Preconditions: none
      // PostConditions: if hasNext() is false -> none
      // if hasNext() is true -> currentCursor is pointing to the correct value, which is not complete, and shows a valid value
      // the currentRowSignature reflects the value of the currentCursor
      // Return value:
      private boolean populateCursor()
      {
        if (currentCursor != null && !currentCursor.isDone()) {
          return true;
        }

        if (done()) {
          return false;
        }

        // Here we know that scanResultValue() DEFINITELY has a next value, while the currentCursor is either null or is complete

        ScanResultValue scanResultValue = resultSequenceIterator.next();
        final RowSignature rowSignature = scanResultValue.getRowSignature() != null
                                          ? scanResultValue.getRowSignature()
                                          : defaultRowSignature;
        RowSignature modifiedRowSignature = useNestedForUnknownTypes
                                            ? FrameWriterUtils.replaceUnknownTypesWithNestedColumns(rowSignature)
                                            : rowSignature;

        // currentRowSignature at this time points to the previous row's signature
        final boolean compatible = modifiedRowSignature != null
                                   && modifiedRowSignature.equals(currentRowSignature);

        final List rows = (List) scanResultValue.getEvents();
        final Iterable<Object[]> formattedRows = Lists.newArrayList(Iterables.transform(
            rows,
            (Function) resultFormatMapper.apply(modifiedRowSignature)
        ));

        Pair<Cursor, Closeable> cursorAndCloseable = IterableRowsCursorHelper.getCursorFromIterable(
            formattedRows,
            modifiedRowSignature
        );

        currentCursor = cursorAndCloseable.lhs;
        closer.register(cursorAndCloseable.rhs);

        // Donot update the previous rowSignature before ensuring that the cursor is not null
        if (currentCursor.isDone()) {
          return populateCursor();
        }

        previousRowSignature = currentRowSignature;
        currentRowSignature = modifiedRowSignature;
        return compatible;
      }
    };

    // Closer would have the closeables of all the cursors we have created till now.
    return Sequences.simple(retVal).withBaggage(closer);
  }
}
