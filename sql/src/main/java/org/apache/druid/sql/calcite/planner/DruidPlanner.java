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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.rel.DruidConvention;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DruidPlanner implements Closeable
{
  private final Planner planner;
  private final PlannerContext plannerContext;
  private final AuthorizerMapper authorizerMapper;

  public DruidPlanner(
      final Planner planner,
      final PlannerContext plannerContext,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.planner = planner;
    this.plannerContext = plannerContext;
    this.authorizerMapper = authorizerMapper;
  }

  public PlannerResult plan(
      final String sql,
      final HttpServletRequest request
  ) throws SqlParseException, ValidationException, RelConversionException, ForbiddenException
  {
    return plan(sql, Preconditions.checkNotNull(request, "request"), null);
  }

  public PlannerResult plan(
      final String sql,
      final AuthenticationResult authenticationResult
  ) throws SqlParseException, ValidationException, RelConversionException, ForbiddenException
  {
    return plan(sql, null, Preconditions.checkNotNull(authenticationResult, "authenticationResult"));
  }

  private PlannerResult plan(
      final String sql,
      @Nullable final HttpServletRequest request,
      @Nullable final AuthenticationResult authenticationResult
  ) throws SqlParseException, ValidationException, RelConversionException, ForbiddenException
  {
    if (authenticationResult != null && request != null) {
      throw new ISE("Cannot specify both 'request' and 'authenticationResult'");
    }

    SqlExplain explain = null;
    SqlNode parsed = planner.parse(sql);
    if (parsed.getKind() == SqlKind.EXPLAIN) {
      explain = (SqlExplain) parsed;
      parsed = explain.getExplicandum();
    }
    final SqlNode validated = planner.validate(parsed);
    final RelRoot root = planner.rel(validated);

    try {
      return planWithDruidConvention(explain, root, request, authenticationResult);
    }
    catch (RelOptPlanner.CannotPlanException e) {
      // Try again with BINDABLE convention. Used for querying Values, metadata tables, and fallback.
      try {
        return planWithBindableConvention(explain, root, request, authenticationResult);
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
        throw e;
      }
    }
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  @Override
  public void close()
  {
    planner.close();
  }

  private PlannerResult planWithDruidConvention(
      final SqlExplain explain,
      final RelRoot root,
      @Nullable final HttpServletRequest request,
      @Nullable final AuthenticationResult authenticationResult
  ) throws RelConversionException, ForbiddenException
  {
    final DruidRel<?> druidRel = (DruidRel<?>) planner.transform(
        Rules.DRUID_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(DruidConvention.instance())
               .plus(root.collation),
        root.rel
    );

    List<String> datasourceNames = druidRel.getDataSourceNames();
    // we'll eventually run a second authorization check at QueryLifecycle.runSimple(), so store the
    // authentication result in the planner context.
    Access authResult;
    if (request != null) {
      authResult = AuthorizationUtils.authorizeAllResourceActions(
          request,
          Iterables.transform(datasourceNames, AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
          authorizerMapper
      );
      plannerContext.setAuthenticationResult(
          (AuthenticationResult) request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)
      );
    } else {
      authResult = AuthorizationUtils.authorizeAllResourceActions(
          authenticationResult,
          Iterables.transform(datasourceNames, AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
          authorizerMapper
      );
      plannerContext.setAuthenticationResult(authenticationResult);
    }

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    if (explain != null) {
      return planExplanation(druidRel, explain);
    } else {
      final Supplier<Sequence<Object[]>> resultsSupplier = new Supplier<Sequence<Object[]>>()
      {
        @Override
        public Sequence<Object[]> get()
        {
          if (root.isRefTrivial()) {
            return druidRel.runQuery();
          } else {
            // Add a mapping on top to accommodate root.fields.
            return Sequences.map(
                druidRel.runQuery(),
                new Function<Object[], Object[]>()
                {
                  @Override
                  public Object[] apply(final Object[] input)
                  {
                    final Object[] retVal = new Object[root.fields.size()];
                    for (int i = 0; i < root.fields.size(); i++) {
                      retVal[i] = input[root.fields.get(i).getKey()];
                    }
                    return retVal;
                  }
                }
            );
          }
        }
      };
      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  private Access authorizeBindableRel(
      BindableRel rel,
      final PlannerContext plannerContext,
      HttpServletRequest req,
      final AuthenticationResult authenticationResult
  )
  {
    Set<String> datasourceNames = Sets.newHashSet();
    rel.childrenAccept(
        new RelVisitor()
        {
          @Override
          public void visit(RelNode node, int ordinal, RelNode parent)
          {
            if (node instanceof DruidRel) {
              datasourceNames.addAll(((DruidRel) node).getDataSourceNames());
            }
            if (node instanceof Bindables.BindableTableScan) {
              Bindables.BindableTableScan bts = (Bindables.BindableTableScan) node;
              RelOptTable table = bts.getTable();
              String tableName = table.getQualifiedName().get(0);
              datasourceNames.add(tableName);
            }
            node.childrenAccept(this);
          }
        }
    );
    if (req != null) {
      plannerContext.setAuthenticationResult(
          (AuthenticationResult) req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)
      );
      return AuthorizationUtils.authorizeAllResourceActions(
          req,
          Iterables.transform(datasourceNames, AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
          authorizerMapper
      );
    } else {
      plannerContext.setAuthenticationResult(authenticationResult);
      return AuthorizationUtils.authorizeAllResourceActions(
          authenticationResult,
          Iterables.transform(datasourceNames, AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
          authorizerMapper
      );
    }
  }

  private PlannerResult planWithBindableConvention(
      final SqlExplain explain,
      final RelRoot root,
      final HttpServletRequest request,
      final AuthenticationResult authenticationResult
  ) throws RelConversionException
  {
    BindableRel bindableRel = (BindableRel) planner.transform(
        Rules.BINDABLE_CONVENTION_RULES,
        planner.getEmptyTraitSet()
               .replace(BindableConvention.INSTANCE)
               .plus(root.collation),
        root.rel
    );

    if (!root.isRefTrivial()) {
      // Add a projection on top to accommodate root.fields.
      final List<RexNode> projects = new ArrayList<>();
      final RexBuilder rexBuilder = bindableRel.getCluster().getRexBuilder();
      for (int field : Pair.left(root.fields)) {
        projects.add(rexBuilder.makeInputRef(bindableRel, field));
      }
      bindableRel = new Bindables.BindableProject(
          bindableRel.getCluster(),
          bindableRel.getTraitSet(),
          bindableRel,
          projects,
          root.validatedRowType
      );
    }

    Access accessResult = authorizeBindableRel(bindableRel, plannerContext, request, authenticationResult);
    if (!accessResult.isAllowed()) {
      throw new ForbiddenException(accessResult.toString());
    }

    if (explain != null) {
      return planExplanation(bindableRel, explain);
    } else {
      final BindableRel theRel = bindableRel;
      final DataContext dataContext = plannerContext.createDataContext((JavaTypeFactory) planner.getTypeFactory());
      final Supplier<Sequence<Object[]>> resultsSupplier = new Supplier<Sequence<Object[]>>()
      {
        @Override
        public Sequence<Object[]> get()
        {
          final Enumerable enumerable = theRel.bind(dataContext);
          return Sequences.simple(enumerable);
        }
      };
      return new PlannerResult(resultsSupplier, root.validatedRowType);
    }
  }

  private PlannerResult planExplanation(
      final RelNode rel,
      final SqlExplain explain
  )
  {
    final String explanation = RelOptUtil.dumpPlan("", rel, explain.getFormat(), explain.getDetailLevel());
    final Supplier<Sequence<Object[]>> resultsSupplier = Suppliers.ofInstance(
        Sequences.simple(ImmutableList.of(new Object[]{explanation})));
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    return new PlannerResult(
        resultsSupplier,
        typeFactory.createStructType(
            ImmutableList.of(Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)),
            ImmutableList.of("PLAN")
        )
    );
  }
}
