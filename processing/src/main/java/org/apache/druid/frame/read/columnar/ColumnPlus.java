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

package org.apache.druid.frame.read.columnar;

import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ValueTypes;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;

import javax.annotation.Nullable;

/**
 * Returned by {@link FrameColumnReader#readColumn}.
 */
public class ColumnPlus implements ColumnHolder
{
  private final BaseColumn column;
  private final ColumnCapabilities capabilities;
  private final int length;

  ColumnPlus(final BaseColumn column, final ColumnCapabilities capabilities, final int length)
  {
    this.column = column;
    this.capabilities = capabilities;
    this.length = length;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return capabilities;
  }

  @Override
  public int getLength()
  {
    return length;
  }

  @Override
  public BaseColumn getColumn()
  {
    return column;
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier()
  {
    return NoIndexesColumnIndexSupplier.getInstance();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public SettableColumnValueSelector makeNewSettableColumnValueSelector()
  {
    return ValueTypes.makeNewSettableColumnValueSelector(getCapabilities().getType());
  }
}
