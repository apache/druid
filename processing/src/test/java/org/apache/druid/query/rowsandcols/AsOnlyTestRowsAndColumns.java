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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.column.Column;

import java.util.Collection;

public abstract class AsOnlyTestRowsAndColumns implements RowsAndColumns
{
  private final WindowFrame theFrame;
  private final AggregatorFactory[] theAggs;
  private final MapOfColumnsRowsAndColumns rac;

  public AsOnlyTestRowsAndColumns(
      WindowFrame theFrame,
      AggregatorFactory[] theAggs,
      MapOfColumnsRowsAndColumns rac
  )
  {
    this.theFrame = theFrame;
    this.theAggs = theAggs;
    this.rac = rac;
  }

  @Override
  public Collection<String> getColumnNames()
  {
    throw new UOE("not called");
  }

  @Override
  public int numRows()
  {
    throw new UOE("not called");
  }

  @Override
  public Column findColumn(String name)
  {
    throw new UOE("not called");
  }
}
