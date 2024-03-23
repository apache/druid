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

package org.apache.druid.query.rowsandcols.concrete;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.columnar.FrameColumnReaders;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashMap;

public class FrameRowsAndColumns implements RowsAndColumns, AutoCloseable, CloseableShapeshifter
{
  private final Frame frame;
  private final RowSignature signature;
  private final LinkedHashMap<String, Column> colCache = new LinkedHashMap<>();

  public FrameRowsAndColumns(Frame frame, RowSignature signature)
  {
    this.frame = FrameType.COLUMNAR.ensureType(frame);
    this.signature = signature;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    return signature.getColumnNames();
  }

  @Override
  public int numRows()
  {
    return frame.numRows();
  }

  @Nullable
  @Override
  public Column findColumn(String name)
  {
    // Use contains so that we can negative cache.
    if (!colCache.containsKey(name)) {
      final int columnIndex = signature.indexOf(name);
      if (columnIndex < 0) {
        colCache.put(name, null);
      } else {
        final ColumnType columnType = signature
            .getColumnType(columnIndex)
            .orElseThrow(() -> new ISE("just got the id, why is columnType not there?"));

        colCache.put(name, FrameColumnReaders.create(name, columnIndex, columnType).readRACColumn(frame));
      }
    }
    return colCache.get(name);

  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (StorageAdapter.class.equals(clazz)) {
      return (T) new FrameStorageAdapter(frame, FrameReader.create(signature), Intervals.ETERNITY);
    }
    return null;
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
