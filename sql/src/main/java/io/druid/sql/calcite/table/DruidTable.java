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
import io.druid.query.DataSource;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.rel.DruidQueryRel;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

import java.util.Map;

public class DruidTable implements TranslatableTable
{
  private final QueryMaker queryMaker;
  private final DataSource dataSource;
  private final RowSignature rowSignature;

  public DruidTable(
      final QueryMaker queryMaker,
      final DataSource dataSource,
      final Map<String, ValueType> columns
  )
  {
    this.queryMaker = Preconditions.checkNotNull(queryMaker, "queryMaker");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");

    final RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    for (Map.Entry<String, ValueType> entry : ImmutableSortedMap.copyOf(columns).entrySet()) {
      rowSignatureBuilder.add(entry.getKey(), entry.getValue());
    }
    this.rowSignature = rowSignatureBuilder.build();
  }

  public QueryMaker getQueryMaker()
  {
    return queryMaker;
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  public RowSignature getRowSignature()
  {
    return rowSignature;
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
    return rowSignature.getRelDataType(typeFactory);
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
    return rowSignature != null ? rowSignature.equals(that.rowSignature) : that.rowSignature == null;
  }

  @Override
  public int hashCode()
  {
    int result = dataSource != null ? dataSource.hashCode() : 0;
    result = 31 * result + (rowSignature != null ? rowSignature.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DruidTable{" +
           "dataSource=" + dataSource +
           ", rowSignature=" + rowSignature +
           '}';
  }
}
