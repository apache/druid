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

package org.apache.druid.sql.calcite.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalTableScan;

/**
 * Represents an source of data external to Druid: a CSV file, an HTTP request, etc.
 * Each such table represents one of Druid's {@link DataSource} types. Since SQL
 * requires knowledge of the schema of that input source, the user must provide
 * that information in SQL (via the `EXTERN` or up-coming `STAGED` function) or
 * from the upcoming Druid Catalog.
 */
public class ExternalTable extends DruidTable
{
  private final DataSource dataSource;
  private final ObjectMapper objectMapper;

  public ExternalTable(
      final DataSource dataSource,
      final RowSignature rowSignature,
      final ObjectMapper objectMapper
  )
  {
    super(rowSignature);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.objectMapper = objectMapper;
  }

  @Override
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @Override
  public boolean isJoinable()
  {
    return false;
  }

  @Override
  public boolean isBroadcast()
  {
    return false;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory)
  {
    // For external datasources, the row type should be determined by whatever the row signature has been explicitly
    // passed in. Typecasting directly to SqlTypeName.TIMESTAMP will lead to inconsistencies with the Calcite functions
    // For example, TIME_PARSE(__time) where __time is specified to be a string field in the external datasource
    // would lead to an exception because __time would be interpreted as timestamp if we typecast it.
    return RowSignatures.toRelDataType(getRowSignature(), typeFactory, true);
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable)
  {
    return new ExternalTableScan(context.getCluster(), objectMapper, this);
  }

  @Override
  public String toString()
  {
    return "ExternalTable{" +
           "dataSource=" + dataSource +
           ", rowSignature=" + getRowSignature() +
           '}';
  }
}
