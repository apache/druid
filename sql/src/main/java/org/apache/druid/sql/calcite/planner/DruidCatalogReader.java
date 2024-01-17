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

import com.google.common.collect.Iterators;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.external.DruidTableMacro;
import org.apache.druid.sql.calcite.external.DruidUserDefinedTableMacro;
import org.apache.druid.sql.calcite.external.Externals;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Druid-specific catalog reader which provides the special processing needed for
 * external table macros which must be wrapped in a Druid-specific operator class.
 */
public class DruidCatalogReader extends CalciteCatalogReader
{
  public DruidCatalogReader(
      final CalciteSchema rootSchema,
      final List<String> defaultSchema,
      final RelDataTypeFactory typeFactory,
      final CalciteConnectionConfig config
  )
  {
    super(rootSchema, defaultSchema, typeFactory, config);
  }

  @Override
  public void lookupOperatorOverloads(final SqlIdentifier opName,
                                      final SqlFunctionCategory category,
                                      final SqlSyntax syntax,
                                      final List<SqlOperator> operatorList,
                                      final SqlNameMatcher nameMatcher
  )
  {
    if (syntax != SqlSyntax.FUNCTION) {
      return;
    }

    // A shame to do it this way...
    // We need our own operator for external table functions. Calcite locks down the
    // mapping from functions to operators and does not provide a way for us to
    // customize. So, we have to intercept external schema lookups and do our own
    // conversion to operators.
    if (isExt(opName, nameMatcher)) {
      lookupExtOperator(opName, operatorList, nameMatcher);
    } else {
      // Look in some other schema.
      super.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
    }
  }

  private boolean isExt(
      SqlIdentifier opName,
      SqlNameMatcher nameMatcher
  )
  {
    if (opName.names.size() != 2) {
      return false;
    }
    return nameMatcher.matches(TableId.EXTERNAL_SCHEMA, opName.names.get(0));
  }

  /**
   * Resolve an external table as a function. The table may be "complete" which means it
   * takes zero arguments. We allow this, it is OK, if verbose, to say
   * {@code FROM(TABLE(ext.foo())} instead of the simpler {@code FROM ext.foo}. Most
   * external tables are partial, meaning that they need extra parameters a query time,
   * which means that, in Calcite terms, they are table functions. We wrap those functions
   * in a Druid specific table macro, then wrap that macro in a Druid-specific operator
   * which is {@code EXTEND}-aware so that we can pass the schema into the function.
   */
  private void lookupExtOperator(SqlIdentifier opName, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher)
  {
    CalciteSchema schema =
        SqlValidatorUtil.getSchema(rootSchema, opName.names.subList(0, 1), nameMatcher);
    if (schema == null) {
      return;
    }
    Collection<Function> fns = schema.getFunctions(opName.names.get(1), nameMatcher.isCaseSensitive());
    if (fns.isEmpty()) {
      return;
    }
    if (fns.size() > 1) {
      throw new ISE("Found multiple table functions for %s", opName.toString());
    }
    Function fn = Iterators.getOnlyElement(fns.iterator());
    if (!(fn instanceof DruidTableMacro)) {
      throw new ISE("Table function %s is not a DruidTableMacro", opName.toString());
    }
    operatorList.add(new ExternalUserDefinedTableMacro(opName, (DruidTableMacro) fn));
  }

  /**
   * Druid user defined table macro that preserves the full function name so that
   * later re-resolutions of this function will search only in the ext schema
   * rather than also searching in all schemas. (By default, Calcite preserves only
   * the tail of the name, not the prefix.)
   * <p>
   * Also, we want the auth resource to use the actual table name, not just the
   * generic `EXTERNAL`.
   */
  public static class ExternalUserDefinedTableMacro extends DruidUserDefinedTableMacro
  {
    private final SqlIdentifier fullName;

    public ExternalUserDefinedTableMacro(SqlIdentifier name, DruidTableMacro macro)
    {
      super(macro);
      this.fullName = name;
    }

    @Override
    public SqlIdentifier getNameAsId()
    {
      return fullName;
    }

    @Override
    public String toString()
    {
      return fullName.toString();
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call, final boolean inputSourceTypeSecurityEnabled)
    {
      return Collections.singleton(Externals.externalRead(fullName.names.get(fullName.names.size() - 1)));
    }
  }
}
