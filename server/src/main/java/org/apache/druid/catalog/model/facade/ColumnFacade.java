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

package org.apache.druid.catalog.model.facade;

import org.apache.druid.catalog.model.ColumnDefn.ResolvedColumn;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.segment.column.ColumnType;

public class ColumnFacade
{
  public static class DatasourceColumnFacade extends ColumnFacade
  {
    public DatasourceColumnFacade(ResolvedColumn column)
    {
      super(column);
    }

    @Override
    public ColumnType druidType()
    {
      if (Columns.isTimeColumn(column.spec().name())) {
        return ColumnType.LONG;
      }
      return super.druidType();
    }
  }

  protected final ResolvedColumn column;

  public ColumnFacade(ResolvedColumn column)
  {
    this.column = column;
  }

  public ColumnType druidType()
  {
    String sqlType = column.spec().sqlType();
    return sqlType == null ? null : Columns.druidType(sqlType);
  }
}
