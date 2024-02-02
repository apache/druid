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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Walks an {@link SqlNode} to collect a set of {@link Resource} for {@link ResourceType#DATASOURCE} and
 * {@link ResourceType#VIEW} to use for authorization during query planning.
 *
 * It works by looking for {@link SqlIdentifier} which correspond to a {@link IdentifierNamespace}, where
 * {@link SqlValidatorNamespace} is calcite-speak for sources of data and {@link IdentifierNamespace} specifically are
 * namespaces which are identified by a single variable, e.g. table names.
 */
public class SqlResourceCollectorShuttle extends SqlShuttle
{
  private final Set<ResourceAction> resourceActions;
  private final PlannerContext plannerContext;
  private final SqlValidator validator;

  public SqlResourceCollectorShuttle(SqlValidator validator, PlannerContext plannerContext)
  {
    this.validator = validator;
    this.resourceActions = new HashSet<>();
    this.plannerContext = plannerContext;
  }

  @Override
  public SqlNode visit(SqlCall call)
  {
    SqlOperator operator = call.getOperator();
    if (operator instanceof AuthorizableOperator) {
      resourceActions.addAll(((AuthorizableOperator) operator).computeResources(
          call,
          plannerContext.getPlannerToolbox().getAuthConfig().isEnableInputSourceSecurity()
      ));
    } else if (operator instanceof SqlUserDefinedTableMacro) {
      // This case is unfortunate: we have a table macro inside of a Calcite
      // "user-defined" table macro. The Calcite object won't let us access the Druid
      // object which could give us permissions. So, we have to reverse-engineer permissions
      // from all we are allowed to see, which is the identifier.
      final SqlIdentifier id = ((SqlFunction) operator).getSqlIdentifier();
      visitIdentifier(id.names);
    }

    return super.visit(call);
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
        visitIdentifier(validatorTable.getQualifiedName());
      }
    }
    return super.visit(id);
  }

  private void visitIdentifier(List<String> qualifiedNameParts)
  {
    // 'schema'.'identifier'
    if (qualifiedNameParts.size() == 2) {
      final String schema = qualifiedNameParts.get(0);
      final String resourceName = qualifiedNameParts.get(1);
      final String resourceType = plannerContext.getSchemaResourceType(schema, resourceName);
      if (resourceType != null) {
        resourceActions.add(new ResourceAction(new Resource(resourceName, resourceType), Action.READ));
      }
    } else if (qualifiedNameParts.size() > 2) {
      // Don't expect to see more than 2 names (catalog?).
      throw new ISE("Cannot analyze table identifier %s", qualifiedNameParts);
    }
  }

  public Set<ResourceAction> getResourceActions()
  {
    return resourceActions;
  }
}
