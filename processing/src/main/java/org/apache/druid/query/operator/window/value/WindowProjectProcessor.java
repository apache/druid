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
import org.apache.druid.error.DruidException;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;
import org.apache.druid.query.rowsandcols.semantic.AppendableRowsAndColumns;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.filter.AllNullColumnSelectorFactory;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

public class WindowProjectProcessor implements Processor
{
  private VirtualColumn virtualColumn;

  @JsonCreator
  public WindowProjectProcessor(
      @JsonProperty("virtualColumn") VirtualColumn virtualColumn)
  {
    this.virtualColumn = virtualColumn;

    if (!(virtualColumn instanceof ExpressionVirtualColumn)) {
      DruidException.defensive("Only ExpressionVirtualColumn supported [%s]", virtualColumn);
    }
  }

  @JsonProperty("virtualColumn")
  public VirtualColumn getVirtualColumn()
  {
    return virtualColumn;
  }

  public ColumnType getType()
  {
    return ((ExpressionVirtualColumn) virtualColumn).getOutputType();
  }

  @Override
  public RowsAndColumns process(RowsAndColumns incomingPartition)
  {
    final AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(incomingPartition);

    ColumnValueSelector<?> vv = virtualColumn.makeColumnValueSelector(null, new AllNullColumnSelectorFactory());

    Column column = new ConstantObjectColumn(
        vv.getObject(),
        incomingPartition.numRows(),
        getType());
    retVal.addColumn(
        virtualColumn.getOutputName(),
        column);
    return retVal;
  }

  @Override
  public boolean validateEquivalent(Processor otherProcessor)
  {
    if (otherProcessor instanceof WindowProjectProcessor) {
      WindowProjectProcessor o = (WindowProjectProcessor) otherProcessor;
      return o.virtualColumn.equals(virtualColumn);
    }
    return false;
  }
}
