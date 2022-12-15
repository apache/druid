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

package org.apache.druid.query.operator.window.ranking;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.segment.column.ColumnType;

public class WindowRowNumberProcessor implements Processor
{
  private final String outputColumn;

  @JsonCreator
  public WindowRowNumberProcessor(
      @JsonProperty("outputColumn") String outputColumn
  )
  {
    this.outputColumn = outputColumn;
  }

  @JsonProperty("outputColumn")
  public String getOutputColumn()
  {
    return outputColumn;
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);
    retVal.addColumn(
        outputColumn,
        new ColumnAccessorBasedColumn(
            new ColumnAccessor()
            {
              @Override
              public ColumnType getType()
              {
                return ColumnType.LONG;
              }

              @Override
              public int numRows()
              {
                return incomingPartition.numRows();
              }

              @Override
              public boolean isNull(int rowNum)
              {
                return false;
              }

              @Override
              public Object getObject(int rowNum)
              {
                return getInt(rowNum);
              }

              @Override
              public double getDouble(int rowNum)
              {
                return getInt(rowNum);
              }

              @Override
              public float getFloat(int rowNum)
              {
                return getInt(rowNum);
              }

              @Override
              public long getLong(int rowNum)
              {
                return getInt(rowNum);
              }

              @Override
              public int getInt(int rowNum)
              {
                // cell is 0-indexed, rowNumbers are 1-indexed, so add 1.
                return rowNum + 1;
              }

              @Override
              public int compareRows(int lhsRowNum, int rhsRowNum)
              {
                return Integer.compare(lhsRowNum, rhsRowNum);
              }
            }
        )
    );
    return retVal;
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    return otherProcessor instanceof WindowRowNumberProcessor;
  }

  @Override
  public String toString()
  {
    return "WindowRowNumberProcessor{" +
           "outputColumn='" + outputColumn + '\'' +
           '}';
  }
}
