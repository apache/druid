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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;

import javax.annotation.Nullable;
import java.util.Objects;

public class WindowLastProcessor extends WindowValueProcessorBase
{
  @Nullable
  private final WindowFrame frame;

  @JsonCreator
  public WindowLastProcessor(
      @JsonProperty("inputColumn") String inputColumn,
      @JsonProperty("outputColumn") String outputColumn,
      @JsonProperty("frame") @Nullable WindowFrame frame
  )
  {
    super(inputColumn, outputColumn);
    this.frame = frame;
  }

  @Nullable
  @JsonProperty("frame")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public WindowFrame getFrame()
  {
    return frame;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns input)
  {
    final int numRows = input.numRows();

    if (numRows == 0) {
      throw DruidException.defensive("Called with an input partition of size 0.  The call site needs to not do that.");
    }

    if (frame == null) {
      final int lastIndex = numRows - 1;
      return processInternal(
          input,
          column -> {
            final ColumnAccessor accessor = column.toAccessor();
            return new ConstantObjectColumn(accessor.getObject(lastIndex), accessor.numRows(), accessor.getType());
          }
      );
    }

    return processInternal(
        input,
        column -> {
          final ColumnAccessor accessor = column.toAccessor();
          final Object[] results = new Object[numRows];
          computeFirstOrLastValueFramed(input, accessor, frame, results, false);
          return new ObjectArrayColumn(results, accessor.getType());
        }
    );
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowLastProcessor) {
      final WindowLastProcessor other = (WindowLastProcessor) otherProcessor;
      return Objects.equals(frame, other.frame) && intervalValidation(other);
    }
    return false;
  }
}
