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

package org.apache.druid.sql.calcite.schema;


import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.DefaultEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.setting.QuerySettingRegistry;
import org.apache.druid.setting.SettingEntry;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.Iterator;

public class QuerySettingTable extends AbstractTable implements ScannableTable
{
  static final String TABLE_NAME = "query_settings";

  static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add("name", ColumnType.STRING)
      .add("type", ColumnType.STRING)
      .add("default_value", ColumnType.STRING)
      .add("deprecated", ColumnType.LONG)
      .add("description", ColumnType.STRING)
      .build();

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(ROW_SIGNATURE, typeFactory);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root)
  {
    return new DataEnumerable(QuerySettingRegistry.getInstance()
                                                  .getSettings()
                                                  .iterator());
  }

  static class DataEnumerable extends DefaultEnumerable<Object[]>
  {
    private final Iterator<SettingEntry<?>> it;

    DataEnumerable(Iterator<SettingEntry<?>> settingIterator)
    {
      this.it = settingIterator;
    }

    @Override
    public Iterator<Object[]> iterator()
    {
      throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
    }

    @Override
    public Enumerator<Object[]> enumerator()
    {
      return new Enumerator<Object[]>()
      {
        @Override
        public Object[] current()
        {
          final SettingEntry<?> task = it.next();
          return new Object[]{
              task.name(),
              task.type(),
              task.defaultValue() == null ? null : task.defaultValue().toString(),
              task.deprecated() ? 1L : 0L,
              task.description()
          };
        }

        @Override
        public boolean moveNext()
        {
          return it.hasNext();
        }

        @Override
        public void reset()
        {
        }

        @Override
        public void close()
        {
        }
      };
    }
  }
}
