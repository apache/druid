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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;

import java.util.Collections;
import java.util.Set;

/**
 * Base class for input-source-specific table functions with arguments derived from
 * a catalog external table definition. Such functions work in conjunction with the
 * EXTEND key word to provide a schema. Example of the HTTP form:
 * <code><pre>
 * INSERT INTO myTable SELECT ...
 * FROM TABLE(http(
 *     userName => 'bob',
 *     password => 'secret',
 *     uris => ARRAY['http:foo.com/bar.csv'],
 *     format => 'csv'))
 *   EXTEND (x VARCHAR, y VARCHAR, z BIGINT)
 * PARTITIONED BY ...
 * </pre></code>
 * <p>
 * This version
 */
public class DruidUserDefinedTableMacro extends SchemaAwareUserDefinedTableMacro implements AuthorizableOperator
{
  public DruidUserDefinedTableMacro(final DruidTableMacro macro)
  {
    super(
        new SqlIdentifier(macro.name, SqlParserPos.ZERO),
        ReturnTypes.CURSOR,
        null,
        // Use our own definition of variadic since Calcite's doesn't allow
        // optional parameters.
        Externals.variadic(macro.parameters),
        macro
    );
  }

  @Override
  public Set<ResourceAction> computeResources(final SqlCall call, final boolean inputSourceTypeSecurityEnabled)
  {
    return Collections.singleton(Externals.EXTERNAL_RESOURCE_ACTION);
  }
}
