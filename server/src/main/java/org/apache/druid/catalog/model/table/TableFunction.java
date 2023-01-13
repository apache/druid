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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ColumnSpec;

import java.util.List;
import java.util.Map;

/**
 * Representation of a SQL table function. The SQL module is not visible here.
 * To avoid unnecessary dependencies, this class provides the information that
 * Calcite needs, but without Calcite dependencies. Each function defines some
 * number of arguments needed to create an external table. This same class
 * can define a "from scratch" table, or to convert a partial table definition
 * in the catalog into a full-defined external table.
 * <p>
 * The parameters tell Calcite the allowed parameters. Functions created from
 * this class are most useful when used with named arguments:<code><pre>
 * SELECT ... FROM TABLE(thisFn(foo -> "bar", answer -> 42))
 * </pre</code>
 * <p>
 * Calcite provides actual arguments as an array, with null values for arguments
 * which the user did not provide. That form is not helpful for this use case.
 * The caller converts those a map, with only the actual arguments set. The
 * call also provides a row schema, obtained from the Calcite {@code EXTEND}
 * extension. The result is an {@link ExternalTableSpec} which the caller uses
 * to create the Calcite form of an external table.
 */
public interface TableFunction
{
  enum ParameterType
  {
    VARCHAR("VARCHAR"),
    BIGINT("BIGINT"),
    BOOLEAN("BOOLEAN"),
    VARCHAR_ARRAY("VARCHAR array");

    private final String name;

    ParameterType(String name)
    {
      this.name = name;
    }

    public String sqlName()
    {
      return name;
    }
  }

  interface ParameterDefn
  {
    String name();
    ParameterType type();
    boolean isOptional();
  }

  List<ParameterDefn> parameters();

  ExternalTableSpec apply(
      String fnName,
      Map<String, Object> args,
      List<ColumnSpec> columns,
      ObjectMapper jsonMapper
  );
}
