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

package org.apache.druid.query.operator.window.value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;

public class WindowOffsetProcessor extends WindowValueProcessorBase
{
  private final int offset;

  @JsonCreator
  public WindowOffsetProcessor(
      @JsonProperty("inputColumn") String inputColumn,
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("offset") int offset
  )
  {
    super(inputColumn, outputColumn);
    this.offset = offset;
  }

  @JsonProperty("offset")
  public int getOffset()
  {
    return offset;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns input)
  {
    final int numRows = input.numRows();

    return processInternal(input, column -> new ColumnAccessorBasedColumn(
        new ShiftedColumnAccessorBase(column.toAccessor())
        {
          @Override
          protected int getActualValue(int rowNum)
          {
            return rowNum + offset;
          }

          @Override
          protected boolean outsideBounds(int rowNum)
          {
            return rowNum < 0 || rowNum >= numRows;
          }
        }));
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowOffsetProcessor) {
      WindowOffsetProcessor other = (WindowOffsetProcessor) otherProcessor;
      return offset == other.offset && intervalValidation(other);
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "WindowOffsetProcessor{" +
           internalToString() +
           ", offset=" + offset +
           '}';
  }
}
