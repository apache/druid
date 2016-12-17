/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.query.DataSource;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

public class DruidTable implements TranslatableTable
{
  private final QuerySegmentWalker walker;
  private final DataSource dataSource;
  private final PlannerConfig config;
  private final Map<String, Integer> columnNumbers;
  private final List<ValueType> columnTypes;
  private final List<String> columnNames;

  public DruidTable(
      final QuerySegmentWalker walker,
      final DataSource dataSource,
      final PlannerConfig config,
      final Map<String, ValueType> columns
  )
  {
    this.walker = Preconditions.checkNotNull(walker, "walker");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.config = Preconditions.checkNotNull(config, "config");
    this.columnNumbers = Maps.newLinkedHashMap();
    this.columnTypes = Lists.newArrayList();
    this.columnNames = Lists.newArrayList();

    int i = 0;
    for (Map.Entry<String, ValueType> entry : ImmutableSortedMap.copyOf(columns).entrySet()) {
      columnNumbers.put(entry.getKey(), i++);
      columnTypes.add(entry.getValue());
      columnNames.add(entry.getKey());
    }
  }

  public QuerySegmentWalker getQuerySegmentWalker()
  {
    return walker;
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  public PlannerConfig getPlannerConfig()
  {
    return config;
  }

  public int getColumnCount()
  {
    return columnNames.size();
  }

  public int getColumnNumber(final String name)
  {
    final Integer number = columnNumbers.get(name);
    return number != null ? number : -1;
  }

  public String getColumnName(final int n)
  {
    return columnNames.get(n);
  }

  public ValueType getColumnType(final int n)
  {
    return columnTypes.get(n);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.TABLE;
  }

  @Override
  public Statistic getStatistic()
  {
    return Statistics.UNKNOWN;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory)
  {
    final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    for (Map.Entry<String, Integer> entry : columnNumbers.entrySet()) {
      final RelDataType sqlTypeName;

      if (entry.getKey().equals(Column.TIME_COLUMN_NAME)) {
        sqlTypeName = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      } else {
        final ValueType valueType = columnTypes.get(entry.getValue());
        switch (valueType) {
          case STRING:
            // Note that there is no attempt here to handle multi-value in any special way. Maybe one day...
            sqlTypeName = typeFactory.createSqlType(SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED);
            break;
          case LONG:
            sqlTypeName = typeFactory.createSqlType(SqlTypeName.BIGINT);
            break;
          case FLOAT:
            sqlTypeName = typeFactory.createSqlType(SqlTypeName.FLOAT);
            break;
          default:
            throw new ISE("WTF?! valueType[%s] not translatable?", valueType);
        }
      }

      builder.add(entry.getKey(), sqlTypeName);
    }
    return builder.build();
  }

  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
  {
    final RelOptCluster cluster = context.getCluster();
    return DruidQueryRel.fullScan(
        cluster,
        cluster.traitSet(),
        table,
        this
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DruidTable that = (DruidTable) o;

    if (dataSource != null ? !dataSource.equals(that.dataSource) : that.dataSource != null) {
      return false;
    }
    if (columnNumbers != null ? !columnNumbers.equals(that.columnNumbers) : that.columnNumbers != null) {
      return false;
    }
    return columnTypes != null ? columnTypes.equals(that.columnTypes) : that.columnTypes == null;

  }

  @Override
  public int hashCode()
  {
    int result = dataSource != null ? dataSource.hashCode() : 0;
    result = 31 * result + (columnNumbers != null ? columnNumbers.hashCode() : 0);
    result = 31 * result + (columnTypes != null ? columnTypes.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DruidTable{" +
           "dataSource=" + dataSource +
           ", columnNumbers=" + columnNumbers +
           ", columnTypes=" + columnTypes +
           '}';
  }
}
