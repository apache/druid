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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.sql.calcite.external.SchemaAwareUserDefinedTableMacro.ExtendedTableMacro;

import java.util.List;

/**
 * Table macro which wraps a catalog table function and which accepts
 * a schema from an EXTENDS clause. This macro is wrapped by the
 * {@link DruidUserDefinedTableMacro} operator that itself extends
 * {@link SchemaAwareUserDefinedTableMacro} which interfaces with the
 * extend operator to pass the schema via a "back channel." The plumbing
 * is complex because we're adding functionality a bit outside the SQL
 * standard, and we have to fit our logic into the Calcite stack.
 */
public class DruidTableMacro implements ExtendedTableMacro
{
  protected final String name;
  final List<FunctionParameter> parameters;
  private final TableFunction fn;
  private final ObjectMapper jsonMapper;

  public DruidTableMacro(
      final String name,
      final TableFunction fn,
      final ObjectMapper jsonMapper
  )
  {
    this.name = name;
    this.jsonMapper = jsonMapper;
    this.fn = fn;
    this.parameters = Externals.convertParameters(fn);
  }

  /**
   * Called when the function is used without an {@code EXTEND} clause.
   * {@code EXTERN} allows this, most others do not.
   */
  @Override
  public TranslatableTable apply(final List<?> arguments)
  {
    return apply(arguments, null);
  }

  @Override
  public TranslatableTable apply(List<?> arguments, SqlNodeList schema)
  {
    final ExternalTableSpec externSpec = fn.apply(
        name,
        Externals.convertArguments(fn, arguments),
        schema == null ? null : Externals.convertColumns(schema),
        jsonMapper
    );
    return Externals.buildExternalTable(externSpec, jsonMapper);
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return parameters;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }
}
