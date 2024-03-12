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
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.column.RowSignature;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Returns a thread-unsafe iterable, that converts a sequence of {@link ScanResultValue} to an iterable of {@link FrameSignaturePair}.
 * ScanResultValues can have heterogenous row signatures, and the returned sequence would have batched
 * them into frames appropriately.
 * <p>
 * The batching process greedily merges the values from the scan result values that have the same signature, while
 * still maintaining the manageable frame sizes that is determined by the memory allocator by splitting the rows
 * whenever necessary.
 * <p>
 * It is necessary that we don't batch and store the ScanResultValues somewhere (like a List) while we do this processing
 * to prevent the heap from exhausting, without limit. It has to be done online - as the scan result values get materialized,
 * we produce frames. A few ScanResultValues might be stored however (if the frame got cut off in the middle)
 * <p>
 * Assuming that we have a sequence of scan result values like:
 * <p>
 * ScanResultValue1 - RowSignatureA - 3 rows
 * ScanResultValue2 - RowSignatureB - 2 rows
 * ScanResultValue3 - RowSignatureA - 1 rows
 * ScanResultValue4 - RowSignatureA - 4 rows
 * ScanResultValue5 - RowSignatureB - 3 rows
 * <p>
 * Also, assume that each individual frame can hold two rows (in practice, it is determined by the row size and
 * the memory block allocated by the memory allocator factory)
 * <p>
 * The output would be a sequence like:
 * Frame1 - RowSignatureA - rows 1-2 from ScanResultValue1
 * Frame2 - RowSignatureA - row 3 from ScanResultValue1
 * Frame3 - RowSignatureB - rows 1-2 from ScanResultValue2
 * Frame4 - RowSignatureA - row 1 from ScanResultValue3, row 1 from ScanResultValue4
 * Frame5 - RowSignatureA - row 2-3 from ScanResultValue4
 * Frame6 - RowSignatureA - row 4 from ScanResultValue4
 * Frame7 - RowSignatureB - row 1-2 from ScanResultValue5
 * Frame8 - RowSignatureB - row 3 from ScanResultValue6
 * <p>
 */

public class ScanResultValueFramesIterable implements Iterable<FrameSignaturePair>
{

  final Sequence<ScanResultValue> resultSequence;
  final MemoryAllocatorFactory memoryAllocatorFactory;
  final boolean useNestedForUnknownTypes;
  final RowSignature defaultRowSignature;
  final Function<RowSignature, Function<?, Object[]>> resultFormatMapper;

  public ScanResultValueFramesIterable(
      Sequence<ScanResultValue> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes,
      RowSignature defaultRowSignature,
      Function<RowSignature, Function<?, Object[]>> resultFormatMapper
  )
  {
    this.resultSequence = resultSequence;
    this.memoryAllocatorFactory = memoryAllocatorFactory;
    this.useNestedForUnknownTypes = useNestedForUnknownTypes;
    this.defaultRowSignature = defaultRowSignature;
    this.resultFormatMapper = resultFormatMapper;
  }

  @Override
  public Iterator<FrameSignaturePair> iterator()
  {
    return new ScanResultValueFramesIterator(
        resultSequence,
        memoryAllocatorFactory,
        useNestedForUnknownTypes,
        defaultRowSignature,
        resultFormatMapper
    );
  }

  private static class ScanResultValueFramesIterator implements Iterator<FrameSignaturePair>
  {

    /**
     * Memory allocator factory to use for frame writers
     */
    final MemoryAllocatorFactory memoryAllocatorFactory;

    /**
     * Replace unknown types in the row signature with {@code COMPLEX<JSON>}
     */
    final boolean useNestedForUnknownTypes;

    /**
     * Default row signature to use if the scan result value doesn't cantain any row signature. This will usually be
     * the row signature of the scan query containing only the column names (and no types)
     */
    final RowSignature defaultRowSignature;

    /**
     * Mapper to convert the scan result value to rows
     */
    final Function<RowSignature, Function<?, Object[]>> resultFormatMapper;

    /**
     * Accumulating the closers for all the resources created so far
     */
    final Closer closer = Closer.create();

    /**
     * Iterator from the scan result value's sequence, so that we can fetch the individual values. Closer registers the
     * iterator so that we can clean up any resources held by the sequence's iterator, and prevent leaking
     */
    final ScanResultValueIterator resultSequenceIterator;

    /**
     * Either null, or points to the current non-empty cursor (and the cursor would point to the current row)
     */
    Cursor currentCursor = null;

    /**
     * Row signature of the current row
     */
    RowSignature currentRowSignature = null;


    public ScanResultValueFramesIterator(
        Sequence<ScanResultValue> resultSequence,
        MemoryAllocatorFactory memoryAllocatorFactory,
        boolean useNestedForUnknownTypes,
        RowSignature defaultRowSignature,
        Function<RowSignature, Function<?, Object[]>> resultFormatMapper
    )
    {
      this.memoryAllocatorFactory = memoryAllocatorFactory;
      this.useNestedForUnknownTypes = useNestedForUnknownTypes;
      this.defaultRowSignature = defaultRowSignature;
      this.resultFormatMapper = resultFormatMapper;
      this.resultSequenceIterator = new ScanResultValueIterator(resultSequence);

      closer.register(resultSequenceIterator);

      // Makes sure that we run through all the empty scan result values at the beginning and are pointing to a valid
      // row
      populateCursor();
    }

    @Override
    public boolean hasNext()
    {
      return !done();
    }

    @Override
    public FrameSignaturePair next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException("No more frames to produce. Call `hasNext()` before calling `next()`");
      }

      // It would ensure that the cursor and the currentRowSignature is populated properly before we
      // start all the processing
      populateCursor();
      boolean firstRowWritten = false;
      // While calling populateCursor() repeatedly, currentRowSignature might change. Therefore we store the signature
      // with which we have written the frames
      final RowSignature writtenSignature = currentRowSignature;
      FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
          FrameType.COLUMNAR,
          memoryAllocatorFactory,
          currentRowSignature,
          Collections.emptyList()
      );
      Frame frame;
      try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(new SettableCursorColumnSelectorFactory(
          () -> currentCursor,
          currentRowSignature
      ))) {
        while (populateCursor()) { // Do till we don't have any more rows, or the next row isn't compatible with the current row
          if (!frameWriter.addSelection()) { // Add the cursor's row to the frame, till the frame is full
            break;
          }
          firstRowWritten = true;
          currentCursor.advance();
        }

        if (!firstRowWritten) {
          throw FrameCursorUtils.SUBQUERY_ROW_TOO_LARGE_EXCEPTION;
        }
        frame = Frame.wrap(frameWriter.toByteArray());
      }

      return new FrameSignaturePair(frame, writtenSignature);
    }

    /**
     * Returns true if the iterator has no more frames to produce
     */
    private boolean done()
    {
      return
          (currentCursor == null || currentCursor.isDone())
          // If the current cursor is not done, then we can generate rows from the current cursor itself
          && !(resultSequenceIterator.hasNext()); // If the cursor is done, but we still have more frames in the input
    }

    /**
     * This is the most important method of this iterator. This determines if two consecutive scan result values can
     * be batched or not, populates the value of the {@link #currentCursor} and {@link #currentRowSignature},
     * during the course of the iterator, and facilitates the {@link #next()}
     * <p>
     * Multiple calls to populateCursor, without advancing the {@link #currentCursor} is idempotent. This allows successive
     * calls to this method in next(), done() and hasNext() methods without having any additional logic in the callers
     * <p>
     * Preconditions: none (This method can be called safely any time)
     * <p>
     * Postconditions -
     * if (hasNext()) was false before calling the method - none
     * if (hasNext()) was true before calling the method -
     * 1. {@link #currentCursor} - Points to the cursor with non-empty value (i.e. isDone()) is false, and the cursor points
     * to the next row present in the sequence of the scan result values. This row would get materialized to frame
     * 2. {@link #currentRowSignature} - Row signature of the row.
     * <p>
     * Return value -
     * if (hasNext()) is false before calling the method - returns false
     * if (hasNext()) is true before calling the method - returns true if previousRowSignature == currentRowSignature
     */
    private boolean populateCursor()
    {
      if (currentCursor != null && !currentCursor.isDone()) {
        return true;
      }

      if (done()) {
        return false;
      }

      // At this point, we know that we need to move to the next non-empty cursor, AND it exists, because
      // done() is not false
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

      currentRowSignature = modifiedRowSignature;
      return compatible;
    }
  }
}
