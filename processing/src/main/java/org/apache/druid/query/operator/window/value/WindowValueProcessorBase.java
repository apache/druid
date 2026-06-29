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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public abstract class WindowValueProcessorBase implements Processor
{
  private final String inputColumn;
  private final String outputColumn;

  public WindowValueProcessorBase(
      String inputColumn,
      String outputColumn
  )
  {
    this.inputColumn = inputColumn;
    this.outputColumn = outputColumn;
  }

  @JsonProperty("inputColumn")
  public String getInputColumn()
  {
    return inputColumn;
  }

  @JsonProperty("outputColumn")
  public String getOutputColumn()
  {
    return outputColumn;
  }

  /**
   * This implements the common logic between the various value processors.  It looks like it could be static, but if
   * it is static then the lambda becomes polymorphic.  We keep it as a member method of the base class so taht the
   * JVM can inline it and specialize the lambda
   *
   * @param input incoming RowsAndColumns, as in Processor.process
   * @param fn    function that converts the input column into the output column
   * @return RowsAndColumns, as in Processor.process
   */
  public RowsAndColumns processInternal(RowsAndColumns input, Function<Column, Column> fn)
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(input);

    final Column column = input.findColumn(inputColumn);
    if (column == null) {
      throw new ISE("column[%s] doesn't exist, but window function FIRST wants it to", inputColumn);
    }

    retVal.addColumn(outputColumn, fn.apply(column));
    return retVal;
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    return getClass() == otherProcessor.getClass()
           && intervalValidation((WindowValueProcessorBase) otherProcessor);
  }

  protected boolean intervalValidation(WindowValueProcessorBase other)
  {
    // Only input needs to be the same for the processors to produce equivalent results
    return inputColumn.equals(other.inputColumn);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" + internalToString() + '}';
  }

  protected String internalToString()
  {
    return "inputColumn=" + inputColumn +
           ", outputColumn='" + outputColumn + '\'';
  }

  @Override
  public List<String> getOutputColumnNames()
  {
    return Collections.singletonList(outputColumn);
  }

  /**
   * Computes first or last value for each row respecting the given window frame. For each row, selects
   * either the first or last element of the frame. Entries for rows where the frame is empty are left as null.
   *
   * @param first if true, selects the first element of the frame; if false, selects the last
   */
  static void computeFirstOrLastValueFramed(
      final RowsAndColumns rac,
      final ColumnAccessor accessor,
      final WindowFrame frame,
      final Object[] results,
      final boolean first
  )
  {
    final int numRows = accessor.numRows();
    final WindowFrame.Rows rowsFrame = frame.unwrap(WindowFrame.Rows.class);
    if (rowsFrame != null) {
      final int lower = rowsFrame.getLowerOffsetClamped(numRows);
      final int upper = rowsFrame.getUpperOffsetClamped(numRows);
      for (int i = 0; i < numRows; i++) {
        final int frameLower = Math.max(0, i + lower);
        final int frameUpper = Math.min(numRows - 1, i + upper);
        if (frameLower <= frameUpper) {
          results[i] = accessor.getObject(first ? frameLower : frameUpper);
        }
      }
      return;
    }

    // Note that this logic would not be correct for SQL like:
    //   FIRST_VALUE(x) OVER (ORDER BY t RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)
    // The SQL validator will reject queries with offset-based RANGE frames such as this.
    // See https://github.com/apache/druid/issues/15767 for more details.
    final WindowFrame.Groups groupsFrame = frame.unwrap(WindowFrame.Groups.class);
    if (groupsFrame != null) {
      final int[] boundaries =
          ClusteredGroupPartitioner.fromRAC(rac).computeBoundaries(groupsFrame.getOrderByColumns());
      final int numGroups = boundaries.length - 1;
      final int lower = groupsFrame.getLowerOffsetClamped(numGroups);
      final int upper = groupsFrame.getUpperOffsetClamped(numGroups);

      for (int g = 0; g < numGroups; g++) {
        final int lowerGroup = Math.max(0, g + lower);
        final int upperGroup = Math.min(numGroups - 1, g + upper);
        if (lowerGroup <= upperGroup) {
          final Object value = first
              ? accessor.getObject(boundaries[lowerGroup])
              : accessor.getObject(boundaries[upperGroup + 1] - 1);
          for (int i = boundaries[g]; i < boundaries[g + 1]; i++) {
            results[i] = value;
          }
        }
      }
      return;
    }

    throw DruidException.defensive("Unable to handle WindowFrame[%s]", frame);
  }
}
