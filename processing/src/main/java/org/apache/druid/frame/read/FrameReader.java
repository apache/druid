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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.field.FieldReader;
import org.apache.druid.frame.field.FieldReaders;
import org.apache.druid.frame.key.FrameComparisonWidget;
import org.apache.druid.frame.key.FrameComparisonWidgetImpl;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.read.columnar.FrameColumnReader;
import org.apache.druid.frame.read.columnar.FrameColumnReaders;
import org.apache.druid.frame.segment.columnar.ColumnarFrameCursorFactory;
import org.apache.druid.frame.segment.row.RowFrameCursorFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;

/**
 * Embeds the logic to read frames with a given {@link RowSignature}.
 * Can be shared by multiple threads, to encourage reuse of column and field readers.
 */
public class FrameReader
{
  private final RowSignature signature;

  /**
   * Column readers, for {@link FrameType#isColumnar()}. Lazy initialized. Access with {@link #getColumnReaders()}.
   */
  private volatile List<FrameColumnReader> columnReaders;

  /**
   * Field readers, for {@link FrameType#isRowBased()}. The map is from {@link FrameType} to list of field readers,
   * which is necessary because there are multiple version of {@link FrameType} for row-based frames.
   * Lazy initialized. Access with {@link #getFieldReaders(FrameType)}.
   */
  @GuardedBy("fieldReaders")
  private final EnumMap<FrameType, List<FieldReader>> fieldReaders = new EnumMap<>(FrameType.class);

  private FrameReader(final RowSignature signature)
  {
    this.signature = signature;
  }

  /**
   * Create a reader for frames with a given {@link RowSignature}. The signature must exactly match the frames to be
   * read, or else behavior is undefined.
   *
   * @param signature signature used to generate the reader
   */
  public static FrameReader create(final RowSignature signature)
  {
    // Double-check that the frame does not have any disallowed field names. Generally, we expect this to be
    // caught on the write side, but we do it again here for safety.
    final Set<String> disallowedFieldNames = FrameWriterUtils.findDisallowedFieldNames(signature);
    if (!disallowedFieldNames.isEmpty()) {
      throw new IAE("Disallowed column names[%s]", disallowedFieldNames);
    }

    // Check that the signature does not have any null types.
    for (int columnNumber = 0; columnNumber < signature.size(); columnNumber++) {
      if (!signature.getColumnType(columnNumber).isPresent()) {
        throw DruidException.defensive("Missing type for column[%s]", signature.getColumnName(columnNumber));
      }
    }

    return new FrameReader(signature);
  }

  public RowSignature signature()
  {
    return signature;
  }

  /**
   * Returns capabilities for a particular column in a particular frame.
   * <p>
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
      if (frame.type().isColumnar()) {
        // Better than frameReader.frameSignature().getColumnCapabilities(columnName), because this method has more
        // insight into what's actually going on with this column (nulls, multivalue, etc).
        return getColumnReaders().get(columnNumber).readColumn(frame).getCapabilities();
      }
      return signature.getColumnCapabilities(columnName);
    }
  }

  /**
   * Create a {@link CursorFactory} for the given frame.
   */
  public CursorFactory makeCursorFactory(final Frame frame)
  {
    if (frame.type().isColumnar()) {
      return new ColumnarFrameCursorFactory(frame, signature, getColumnReaders());
    } else {
      return new RowFrameCursorFactory(frame, this, getFieldReaders(frame.type()));
    }
  }

  /**
   * Create a {@link FrameComparisonWidget} for the given frame.
   * <p>
   * Only possible for row-based frames. The provided sortColumns must be a prefix of {@link #signature()}.
   */
  public FrameComparisonWidget makeComparisonWidget(final Frame frame, final List<KeyColumn> keyColumns)
  {
    FrameWriterUtils.verifySortColumns(keyColumns, signature);
    return FrameComparisonWidgetImpl.create(
        frame,
        signature,
        keyColumns,
        getFieldReaders(frame.type()).subList(0, keyColumns.size())
    );
  }

  /**
   * Returns readers for columnar frames, using {@link #columnReaders} as a cache.
   */
  private List<FrameColumnReader> getColumnReaders()
  {
    if (columnReaders == null) {
      synchronized (this) {
        if (columnReaders == null) {
          columnReaders = makeColumnReaders(signature);
        }
      }
    }

    return columnReaders;
  }

  /**
   * Returns readers for row-based frames, using {@link #fieldReaders} as a cache.
   */
  private List<FieldReader> getFieldReaders(final FrameType frameType)
  {
    synchronized (fieldReaders) {
      return fieldReaders.computeIfAbsent(frameType, type -> makeFieldReaders(signature, type));
    }
  }

  /**
   * Helper used by {@link #getColumnReaders()}.
   */
  private static List<FrameColumnReader> makeColumnReaders(final RowSignature signature)
  {
    final List<FrameColumnReader> columnReaders = new ArrayList<>(signature.size());

    for (int columnNumber = 0; columnNumber < signature.size(); columnNumber++) {
      // columnType will not be missing, since it was validated in create().
      final ColumnType columnType = signature.getColumnType(columnNumber).get();
      columnReaders.add(FrameColumnReaders.create(signature.getColumnName(columnNumber), columnNumber, columnType));
    }

    return columnReaders;
  }

  /**
   * Helper used by {@link #getFieldReaders(FrameType)}.
   */
  private static List<FieldReader> makeFieldReaders(final RowSignature signature, final FrameType frameType)
  {
    final List<FieldReader> fieldReaders = new ArrayList<>(signature.size());

    for (int columnNumber = 0; columnNumber < signature.size(); columnNumber++) {
      // columnType will not be missing, since it was validated in create().
      final ColumnType columnType = signature.getColumnType(columnNumber).get();
      fieldReaders.add(FieldReaders.create(signature.getColumnName(columnNumber), columnType, frameType));
    }

    return fieldReaders;
  }
}
