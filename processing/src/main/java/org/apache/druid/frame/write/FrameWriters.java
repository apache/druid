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

import com.google.common.base.Preconditions;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocator;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.write.columnar.ColumnarFrameWriterFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Outward-facing utility methods for {@link FrameWriterFactory} and {@link FrameWriter} users.
 */
public class FrameWriters
{
  private FrameWriters()
  {
    // No instantiation.
  }

  /**
   * Creates a {@link FrameWriterFactory} that produces frames of the given {@link FrameType}.
   */
  public static FrameWriterFactory makeFrameWriterFactory(
      final FrameType frameType,
      final MemoryAllocator allocator,
      final RowSignature signature,
      final List<SortColumn> sortColumns
  )
  {
    switch (Preconditions.checkNotNull(frameType, "frameType")) {
      case COLUMNAR:
        return new ColumnarFrameWriterFactory(allocator, signature, sortColumns);
      case ROW_BASED:
        return new RowBasedFrameWriterFactory(allocator, signature, sortColumns);
      default:
        throw new ISE("Unrecognized frame type [%s]", frameType);
    }
  }

  /**
   * Returns a copy of "signature" with columns rearranged so the provided sortColumns appear as a prefix.
   * Throws an error if any of the sortColumns are not present in the input signature, or if any of their
   * types are unknown.
   *
   * This is useful because sort columns must appear
   */
  public static RowSignature sortableSignature(
      final RowSignature signature,
      final List<SortColumn> sortColumns
  )
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final SortColumn columnName : sortColumns) {
      final Optional<ColumnType> columnType = signature.getColumnType(columnName.columnName());
      if (!columnType.isPresent()) {
        throw new IAE("Column [%s] not present in signature", columnName);
      }

      builder.add(columnName.columnName(), columnType.get());
    }

    final Set<String> sortColumnNames =
        sortColumns.stream().map(SortColumn::columnName).collect(Collectors.toSet());

    for (int i = 0; i < signature.size(); i++) {
      final String columnName = signature.getColumnName(i);
      if (!sortColumnNames.contains(columnName)) {
        builder.add(columnName, signature.getColumnType(i).orElse(null));
      }
    }

    return builder.build();
  }
}
