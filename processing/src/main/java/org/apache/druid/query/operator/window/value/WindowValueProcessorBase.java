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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;

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
}
