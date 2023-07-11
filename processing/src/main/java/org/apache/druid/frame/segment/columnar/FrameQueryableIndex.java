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

package org.apache.druid.frame.segment.columnar;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.columnar.FrameColumnReader;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link QueryableIndex} implementation based on a single columnar {@link Frame}. There is no internal caching
 * of columns here, so callers should generally wrap this in a {@link org.apache.druid.segment.ColumnCache}.
 *
 * This class exists so {@link FrameCursorFactory} can reuse code meant for regular segment-backed
 * {@link QueryableIndex}. Some methods are implemented by throwing {@link UnsupportedOperationException}, wherever
 * it is not expected that those methods are actually going to be needed.
 */
public class FrameQueryableIndex implements QueryableIndex
{
  private final Frame frame;
  private final RowSignature signature;
  private final List<FrameColumnReader> columnReaders;

  FrameQueryableIndex(
      final Frame frame,
      final RowSignature signature,
      final List<FrameColumnReader> columnReaders
  )
  {
    this.frame = FrameType.COLUMNAR.ensureType(frame);
    this.signature = signature;
    this.columnReaders = columnReaders;
  }

  @Override
  public int getNumRows()
  {
    return frame.numRows();
  }

  @Override
  public List<String> getColumnNames()
  {
    return signature.getColumnNames();
  }

  @Nullable
  @Override
  public ColumnHolder getColumnHolder(final String columnName)
  {
    final int columnIndex = signature.indexOf(columnName);

    if (columnIndex < 0) {
      return null;
    } else {
      return columnReaders.get(columnIndex).readColumn(frame);
    }
  }

  @Override
  public Interval getDataInterval()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(signature.getColumnNames());
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    return new RoaringBitmapFactory();
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return null;
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return Collections.emptyMap();
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
