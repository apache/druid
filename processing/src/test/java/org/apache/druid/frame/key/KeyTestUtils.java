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

package org.apache.druid.frame.key;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class KeyTestUtils
{
  private KeyTestUtils()
  {
    // No instantiation.
  }

  /**
   * Create a signature matching {@code sortColumns}, using types from {@code inspector}.
   */
  public static RowSignature createKeySignature(
      final List<SortColumn> sortColumns,
      final ColumnInspector inspector
  )
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final SortColumn sortColumn : sortColumns) {
      final ColumnCapabilities capabilities = inspector.getColumnCapabilities(sortColumn.columnName());
      final ColumnType columnType =
          Optional.ofNullable(capabilities).map(ColumnCapabilities::toColumnType).orElse(null);
      builder.add(sortColumn.columnName(), columnType);
    }

    return builder.build();
  }

  /**
   * Create a {@link RowKey}.
   *
   * @param keySignature signature to use for the keys
   * @param objects      key field values
   */
  public static RowKey createKey(
      final RowSignature keySignature,
      final Object... objects
  )
  {
    final RowBasedColumnSelectorFactory<Object[]> columnSelectorFactory = RowBasedColumnSelectorFactory.create(
        columnName -> {
          final int idx = keySignature.indexOf(columnName);

          if (idx < 0) {
            return arr -> null;
          } else {
            return arr -> arr[idx];
          }
        },
        () -> objects,
        keySignature,
        true,
        false
    );

    final FrameWriterFactory writerFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        HeapMemoryAllocator.unlimited(),
        keySignature,
        Collections.emptyList()
    );

    try (final FrameWriter writer = writerFactory.newFrameWriter(columnSelectorFactory)) {
      writer.addSelection();
      final Frame frame = Frame.wrap(writer.toByteArray());
      final Memory dataRegion = frame.region(RowBasedFrameWriter.ROW_DATA_REGION);
      final byte[] keyBytes = new byte[(int) dataRegion.getCapacity()];
      dataRegion.copyTo(0, WritableMemory.writableWrap(keyBytes), 0, keyBytes.length);
      return RowKey.wrap(keyBytes);
    }
  }
}
