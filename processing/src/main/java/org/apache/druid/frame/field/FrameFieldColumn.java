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

package org.apache.druid.frame.field;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.segment.row.CursorFrameRowPointer;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.data.ReadableOffset;

import java.io.IOException;

public class FrameFieldColumn implements BaseColumn
{

  private final Frame frame;
  private final FieldReader fieldReader;
  private final int fieldNumber;
  private final int fieldCount;

  public FrameFieldColumn(Frame frame, FieldReader fieldReader, int fieldNumber, int fieldCount)
  {
    this.frame = frame;
    this.fieldReader = fieldReader;
    this.fieldNumber = fieldNumber;
    this.fieldCount = fieldCount;
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return fieldReader.makeColumnValueSelector(
        FrameType.ROW_BASED.ensureType(frame).region(RowBasedFrameWriter.ROW_DATA_REGION),
        new RowMemoryFieldPointer(
            FrameType.ROW_BASED.ensureType(frame).region(RowBasedFrameWriter.ROW_DATA_REGION),
            new CursorFrameRowPointer(frame, offset),
            fieldNumber,
            fieldCount
        )
    );
  }

  @Override
  public void close() throws IOException
  {

  }
}
