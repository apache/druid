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

package org.apache.druid.sql.calcite.view;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;

import java.util.List;

public class DruidViewMacro implements TableMacro
{
  private final PlannerFactory plannerFactory;
  private final Escalator escalator;
  private final String viewSql;

  public DruidViewMacro(final PlannerFactory plannerFactory, final Escalator escalator, final String viewSql)
  {
    this.plannerFactory = plannerFactory;
    this.escalator = escalator;
    this.viewSql = viewSql;
  }

  @Override
  public TranslatableTable apply(final List<Object> arguments)
  {
    final RelDataType rowType;
    // Using an escalator here is a hack, but it's currently needed to get the row type. Ideally, some
    // later refactoring would make this unnecessary, since there is no actual query going out herem.
    final AuthenticationResult authenticationResult = escalator.createEscalatedAuthenticationResult();
    try (final DruidPlanner planner = plannerFactory.createPlanner(null, authenticationResult)) {

      rowType = planner.plan(viewSql).rowType();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new ViewTable(
        null,
        RelDataTypeImpl.proto(rowType),
        viewSql,
        ImmutableList.of(DruidSchema.NAME),
        null
    );
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return ImmutableList.of();
  }
}
