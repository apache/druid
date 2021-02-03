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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Walks an {@link SqlNode} to collect a set of {@link Resource} for {@link ResourceType#DATASOURCE} and
 * {@link ResourceType#VIEW} to use for authorization during query planning.
 *
 * It works by looking for {@link SqlIdentifier} which corespond to a {@link IdentifierNamespace}, where
 * {@link SqlValidatorNamespace} is calcite-speak for sources of data and {@link IdentifierNamespace} specifically are
 * namespaces which are identified by a single variable, e.g. table names.
 */
public class SqlResourceCollectorShuttle extends SqlShuttle
{
  private final Set<Resource> resources;
  private final SqlValidator validator;
  private final String druidSchemaName;

  public SqlResourceCollectorShuttle(SqlValidator validator, String druidSchemaName)
  {
    this.validator = validator;
    this.resources = new HashSet<>();
    this.druidSchemaName = druidSchemaName;
  }

  @Override
  public SqlNode visit(SqlIdentifier id)
  {
    // raw tables and views and such will have a IdentifierNamespace
    // since we are scoped to identifiers here, we should only pick up these
    SqlValidatorNamespace namespace = validator.getNamespace(id);
    if (namespace != null && namespace.isWrapperFor(IdentifierNamespace.class)) {
      SqlValidatorTable validatorTable = namespace.getTable();
      // this should not probably be null if the namespace was not null,
      if (validatorTable != null) {
        List<String> qualifiedNameParts = validatorTable.getQualifiedName();
        // 'schema'.'identifier'
        if (qualifiedNameParts.size() == 2) {
          final String schema = qualifiedNameParts.get(0);
          if (druidSchemaName.equals(schema)) {
            resources.add(new Resource(qualifiedNameParts.get(1), ResourceType.DATASOURCE));
          } else if (NamedViewSchema.NAME.equals(schema)) {
            resources.add(new Resource(qualifiedNameParts.get(1), ResourceType.VIEW));
          }
        }
      }
    }
    return super.visit(id);
  }

  public Set<Resource> getResources()
  {
    return resources;
  }
}
