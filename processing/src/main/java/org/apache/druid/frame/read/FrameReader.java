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

package org.apache.druid.frame.read;

import com.google.common.base.Preconditions;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.field.FieldReader;
import org.apache.druid.frame.field.FieldReaders;
import org.apache.druid.frame.key.FrameComparisonWidget;
import org.apache.druid.frame.key.FrameComparisonWidgetImpl;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.columnar.FrameColumnReader;
import org.apache.druid.frame.read.columnar.FrameColumnReaders;
import org.apache.druid.frame.segment.row.FrameCursorFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Embeds the logic to read frames with a given {@link RowSignature}.
 *
 * Stateless and immutable.
 */
public class FrameReader
{
  private final RowSignature signature;

  // Column readers, for columnar frames.
  private final List<FrameColumnReader> columnReaders;

  // Field readers, for row-based frames.
  private final List<FieldReader> fieldReaders;

  private FrameReader(
      final RowSignature signature,
      final List<FrameColumnReader> columnReaders,
      final List<FieldReader> fieldReaders
  )
  {
    this.signature = signature;
    this.columnReaders = columnReaders;
    this.fieldReaders = fieldReaders;
  }

  /**
   * Create a reader for frames with a given {@link RowSignature}. The signature must exactly match the frames to be
   * read, or else behavior is undefined.
   */
  public static FrameReader create(final RowSignature signature)
  {
    // Double-check that the frame does not have any disallowed field names. Generally, we expect this to be
    // caught on the write side, but we do it again here for safety.
    final Set<String> disallowedFieldNames = FrameWriterUtils.findDisallowedFieldNames(signature);
    if (!disallowedFieldNames.isEmpty()) {
      throw new IAE("Disallowed field names: %s", disallowedFieldNames);
    }

    final List<FrameColumnReader> columnReaders = new ArrayList<>(signature.size());
    final List<FieldReader> fieldReaders = new ArrayList<>(signature.size());

    for (int columnNumber = 0; columnNumber < signature.size(); columnNumber++) {
      final ColumnType columnType =
          Preconditions.checkNotNull(
              signature.getColumnType(columnNumber).orElse(null),
              "Type for column [%s]",
              signature.getColumnName(columnNumber)
          );

      columnReaders.add(FrameColumnReaders.create(columnNumber, columnType));
      fieldReaders.add(FieldReaders.create(signature.getColumnName(columnNumber), columnType));
    }

    return new FrameReader(signature, columnReaders, fieldReaders);
  }

  public RowSignature signature()
  {
    return signature;
  }

  /**
   * Returns capabilities for a particular column in a particular frame.
   *
   * Preferred over {@link RowSignature#getColumnCapabilities(String)} when reading a particular frame, because this
   * method has more insight into what's actually going on with that specific frame (nulls, multivalue, etc). The
   * RowSignature version is based solely on type.
   */
  @Nullable
  public ColumnCapabilities columnCapabilities(final Frame frame, final String columnName)
  {
    final int columnNumber = signature.indexOf(columnName);

    if (columnNumber < 0) {
      return null;
    } else {
      switch (frame.type()) {
        case COLUMNAR:
          // Better than frameReader.frameSignature().getColumnCapabilities(columnName), because this method has more
          // insight into what's actually going on with this column (nulls, multivalue, etc).
          return columnReaders.get(columnNumber).readColumn(frame).getCapabilities();
        default:
          return signature.getColumnCapabilities(columnName);
      }
    }
  }

  /**
   * Create a {@link CursorFactory} for the given frame.
   */
  public CursorFactory makeCursorFactory(final Frame frame)
  {
    switch (frame.type()) {
      case COLUMNAR:
        return new org.apache.druid.frame.segment.columnar.FrameCursorFactory(frame, signature, columnReaders);
      case ROW_BASED:
        return new FrameCursorFactory(frame, this, fieldReaders);
      default:
        throw new ISE("Unrecognized frame type [%s]", frame.type());
    }
  }

  /**
   * Create a {@link FrameComparisonWidget} for the given frame.
   *
   * Only possible for frames of type {@link org.apache.druid.frame.FrameType#ROW_BASED}. The provided
   * sortColumns must be a prefix of {@link #signature()}.
   */
  public FrameComparisonWidget makeComparisonWidget(final Frame frame, final List<SortColumn> sortColumns)
  {
    FrameWriterUtils.verifySortColumns(sortColumns, signature);

    // Verify that all sort columns are comparable.
    for (final SortColumn sortColumn : sortColumns) {
      if (!fieldReaders.get(signature.indexOf(sortColumn.columnName())).isComparable()) {
        throw new IAE(
            "Sort column [%s] is not comparable (type = [%s])",
            sortColumn.columnName(),
            signature.getColumnType(sortColumn.columnName()).orElse(null)
        );
      }
    }

    return FrameComparisonWidgetImpl.create(frame, this, sortColumns);
  }
}
