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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.field.FieldReaders;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility methods used by {@link FrameWriter} implementations.
 */
public class FrameWriterUtils
{
  public static final byte NULL_STRING_MARKER = (byte) 0xFF; /* cannot appear in a valid utf-8 byte sequence */
  public static final byte[] NULL_STRING_MARKER_ARRAY = new byte[]{NULL_STRING_MARKER};

  public static final String RESERVED_FIELD_PREFIX = "___druid";

  /**
   * Writes a frame header to a memory locations.
   */
  public static long writeFrameHeader(
      final WritableMemory memory,
      final long startPosition,
      final FrameType frameType,
      final long totalSize,
      final int numRows,
      final int numRegions,
      final boolean permuted
  )
  {
    long currentPosition = startPosition;

    memory.putByte(currentPosition, frameType.version());
    currentPosition += Byte.BYTES;

    memory.putLong(currentPosition, totalSize);
    currentPosition += Long.BYTES;

    memory.putInt(currentPosition, numRows);
    currentPosition += Integer.BYTES;

    memory.putInt(currentPosition, numRegions);
    currentPosition += Integer.BYTES;

    memory.putByte(currentPosition, permuted ? (byte) 1 : (byte) 0);
    currentPosition += Byte.BYTES;

    return currentPosition - startPosition;
  }

  /**
   * Retrieves UTF-8 byte buffers from a {@link DimensionSelector}, which is expected to be the kind of
   * selector you get for an {@code STRING} column.
   *
   * Null strings are returned as {@link #NULL_STRING_MARKER_ARRAY}.
   *
   * @param selector   the selector
   * @param multiValue if true, return an array that corresponds exactly to {@link DimensionSelector#getRow()}.
   *                   if false, always return a single-valued array. In particular, this means [] is
   *                   returned as [NULL_STRING_MARKER_ARRAY].
   */
  public static List<ByteBuffer> getUtf8ByteBuffersFromStringSelector(
      final DimensionSelector selector,
      final boolean multiValue
  )
  {
    final IndexedInts row = selector.getRow();
    final int size = row.size();

    if (multiValue) {
      final List<ByteBuffer> retVal = new ArrayList<>(size);

      for (int i = 0; i < size; i++) {
        retVal.add(getUtf8ByteBufferFromStringSelector(selector, row.get(i)));
      }

      return retVal;
    } else {
      // If !multivalue, always return exactly one buffer.
      if (size == 0) {
        return Collections.singletonList(ByteBuffer.wrap(NULL_STRING_MARKER_ARRAY));
      } else if (size == 1) {
        return Collections.singletonList(getUtf8ByteBufferFromStringSelector(selector, row.get(0)));
      } else {
        throw new ISE("Encountered unexpected multi-value row");
      }
    }
  }

  /**
   * Retrieves UTF-8 byte buffers from a {@link ColumnValueSelector}, which is expected to be the kind of
   * selector you get for an {@code ARRAY<STRING>} column.
   *
   * Null strings are returned as {@link #NULL_STRING_MARKER_ARRAY}.
   */
  public static List<ByteBuffer> getUtf8ByteBuffersFromStringArraySelector(
      @SuppressWarnings("rawtypes") final ColumnValueSelector selector
  )
  {
    Object row = selector.getObject();
    if (row == null) {
      return Collections.singletonList(getUtf8ByteBufferFromString(null));
    } else if (row instanceof String) {
      return Collections.singletonList(getUtf8ByteBufferFromString((String) row));
    }

    final List<ByteBuffer> retVal = new ArrayList<>();
    if (row instanceof List) {
      for (int i = 0; i < ((List<?>) row).size(); i++) {
        retVal.add(getUtf8ByteBufferFromString(((List<String>) row).get(i)));
      }
    } else if (row instanceof Object[]) {
      for (Object value : (Object[]) row) {
        retVal.add(getUtf8ByteBufferFromString((String) value));
      }
    } else if (row instanceof ComparableStringArray) {
      for (String value : ((ComparableStringArray) row).getDelegate()) {
        retVal.add(getUtf8ByteBufferFromString(value));
      }
    } else {
      throw new ISE("Unexpected type %s found", row.getClass().getName());
    }
    return retVal;
  }

  /**
   * Checks the provided signature for any disallowed field names. Returns any that are found.
   */
  public static Set<String> findDisallowedFieldNames(final RowSignature signature)
  {
    return signature.getColumnNames()
                    .stream()
                    .filter(s -> s.startsWith(RESERVED_FIELD_PREFIX))
                    .collect(Collectors.toSet());
  }

  /**
   * Verifies whether the provided sortColumns are all sortable, and are a prefix of the signature. This is required
   * because it allows us to treat the sort key as a chunk of bytes.
   *
   * Exits quietly if the sort columns are OK. Throws an exception if there is a problem.
   *
   * @throws IllegalArgumentException if there is a problem
   */
  public static void verifySortColumns(
      final List<SortColumn> sortColumns,
      final RowSignature signature
  )
  {
    if (!areSortColumnsPrefixOfSignature(sortColumns, signature)) {
      throw new IAE(
          "Sort column [%s] must be a prefix of the signature",
          sortColumns.stream().map(SortColumn::columnName).collect(Collectors.joining(", "))
      );
    }

    // Verify that all sort columns are comparable.
    for (final SortColumn sortColumn : sortColumns) {
      final ColumnType columnType = signature.getColumnType(sortColumn.columnName()).orElse(null);

      if (columnType == null || !FieldReaders.create(sortColumn.columnName(), columnType).isComparable()) {
        throw new IAE("Sort column [%s] is not comparable (type = %s)", sortColumn.columnName(), columnType);
      }
    }
  }

  /**
   * Copies "len" bytes from {@code src.position()} to "dstPosition" in "memory". Does not update the position of src.
   *
   * @throws InvalidNullByteException "allowNullBytes" is true and a null byte is encountered
   */
  public static void copyByteBufferToMemory(
      final ByteBuffer src,
      final WritableMemory dst,
      final long dstPosition,
      final int len,
      final boolean allowNullBytes
  )
  {
    if (src.remaining() < len) {
      throw new ISE("Insufficient source space available");
    }
    if (dst.getCapacity() - dstPosition < len) {
      throw new ISE("Insufficient destination space available");
    }

    final int srcEnd = src.position() + len;
    long q = dstPosition;

    for (int p = src.position(); p < srcEnd; p++, q++) {
      final byte b = src.get(p);

      if (!allowNullBytes && b == 0) {
        throw new InvalidNullByteException();
      }

      dst.putByte(q, b);
    }
  }

  /**
   * Extracts a ByteBuffer from the provided dictionary selector.
   *
   * Null strings are returned as {@link #NULL_STRING_MARKER_ARRAY}.
   */
  private static ByteBuffer getUtf8ByteBufferFromStringSelector(
      final DimensionDictionarySelector selector,
      final int dictionaryId
  )
  {
    if (selector.supportsLookupNameUtf8()) {
      final ByteBuffer buf = selector.lookupNameUtf8(dictionaryId);

      if (buf == null || (NullHandling.replaceWithDefault() && buf.remaining() == 0)) {
        return ByteBuffer.wrap(NULL_STRING_MARKER_ARRAY);
      } else {
        return buf;
      }
    } else {
      return FrameWriterUtils.getUtf8ByteBufferFromString(selector.lookupName(dictionaryId));
    }
  }

  /**
   * Extracts a ByteBuffer from the string. Null strings are returned as {@link #NULL_STRING_MARKER_ARRAY}.
   */
  private static ByteBuffer getUtf8ByteBufferFromString(@Nullable final String data)
  {
    if (NullHandling.isNullOrEquivalent(data)) {
      return ByteBuffer.wrap(NULL_STRING_MARKER_ARRAY);
    } else {
      return ByteBuffer.wrap(StringUtils.toUtf8(data));
    }
  }

  /**
   * Returns whether the provided sortColumns are all a prefix of the signature.
   */
  private static boolean areSortColumnsPrefixOfSignature(
      final List<SortColumn> sortColumns,
      final RowSignature signature
  )
  {
    if (sortColumns.size() > signature.size()) {
      return false;
    }

    for (int i = 0; i < sortColumns.size(); i++) {
      if (!sortColumns.get(i).columnName().equals(signature.getColumnName(i))) {
        return false;
      }
    }

    return true;
  }
}
