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

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.read.columnar.FrameColumnReaders;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

public class ColumnBasedFrameRowsAndColumns extends AbstractFrameRowsAndColumns
{
  public ColumnBasedFrameRowsAndColumns(Frame frame, RowSignature signature)
  {
    super(FrameType.COLUMNAR.ensureType(frame), signature);
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
            .orElseThrow(
                () -> DruidException.defensive(
                    "just got the id [%s][%s], why is columnType not there?",
                    columnIndex,
                    name
                )
            );

        colCache.put(name, FrameColumnReaders.create(name, columnIndex, columnType).readRACColumn(frame));
      }
    }
    return colCache.get(name);
  }
}
